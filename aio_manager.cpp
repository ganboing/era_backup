#include <sys/signalfd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <signal.h>
#include <aio.h>
#include <fcntl.h>
#include <unistd.h>
#include <error.h>
#include <cinttypes>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <error.h>
#include <errno.h>
#include <cassert>
#include <cstdarg>
#include <memory>
#include <set>
#include <map>
#include <vector>
#include <functional>

struct ManagedFd {
	int fd;
	ManagedFd() : fd(-1)
	{
	}
	ManagedFd(int _fd) : fd(_fd)
	{
	}
	int &operator=(int _fd)
	{
		fd = _fd;
		return fd;
	}
	~ManagedFd()
	{
		if (fd != -1)
			close(fd);
	}
};

static inline unsigned debug_level(const char *label)
{
	auto *val = getenv(label);
	if (!val)
		return 0;
	return strtoul(val, nullptr, 0);
}

static inline std::string cppfmt(const char *fmt, ...)
{
	std::string ret;
	va_list vl;
	va_start(vl, fmt);
	int len = vsnprintf(nullptr, 0, fmt, vl);
	va_end(vl);
	assert(len >= 0);
	if (!len)
		return ret;
	ret.resize(len);
	assert(!ret.empty());
	va_start(vl, fmt);
	vsprintf(&ret.front(), fmt, vl);
	va_end(vl);
	return ret;
}

typedef std::unique_ptr<FILE, int (*)(FILE *)> ptr_FILE;

static unsigned const DBG_LEVEL_AIO = debug_level("DEBUG_AIO");

static void DBG_AIO(int level, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

static void DBG_AIO(int level, const char *fmt, ...)
{
	bool prefix = true;
	if (level < 0) {
		prefix = false;
		level = -level;
	}
	if ((unsigned)level > DBG_LEVEL_AIO)
		return;
	va_list vl;
	va_start(vl, fmt);
	if (prefix)
		fputs("[AIO] ", stderr);
	vfprintf(stderr, fmt, vl);
	va_end(vl);
}

static inline bool is_mem_zero(unsigned char *buff, uint64_t len)
{
	unsigned __int128 *buff128 = (unsigned __int128 *)buff;
	len /= sizeof(unsigned __int128);
	uint64_t i = 0;
	for (; i != len && !buff128[i]; ++i)
		;
	return i == len;
}

struct bitvector {
	std::vector<uint64_t> bits;
	size_t size;
	bitvector() : size(0)
	{
	}

	static const size_t intsz = sizeof(uint64_t) * 8;

	void resize_z(size_t newsz)
	{
		size_t old_isz = (size + intsz - 1) / intsz;
		size_t new_isz = (newsz + intsz - 1) / intsz;
		if (new_isz > bits.size()) {
			bits.resize(new_isz);
		}
		for (size_t i = 0; i != old_isz; ++i) {
			bits[i] = 0;
		}
		size = newsz;
	}

	void set(size_t bitn)
	{
		assert(bitn < size);
		bits[bitn / intsz] |= (uint64_t(1) << (bitn % intsz));
	}

	void unset(size_t bitn)
	{
		assert(bitn < size);
		bits[bitn / intsz] &= ~(uint64_t(1) << (bitn % intsz));
	}

	bool test(size_t bitn) const
	{
		assert(bitn < size);
		return bits[bitn / intsz] & (uint64_t(1) << (bitn % intsz));
	}

	void print(FILE *f) const
	{
		for (size_t i = 0, j = (size + intsz - 1) / intsz; i != j; ++i) {
			size_t p = 0, q = size - i * intsz;
			uint64_t val = bits[i];
			if (q > intsz)
				q = intsz;
			for (; p != q; ++p, val >>= 1) {
				fputc((val & 1) ? '!' : '.', f);
			}
		}
	}

	inline size_t __find_n(size_t bitn, bool iset) const
	{
		assert(bitn <= size);
		if (bitn == size)
			return 0;
		//size_t left = size - bitn / intsz * intsz;

		size_t seg_n = bitn / intsz;
		size_t seg_shift = bitn % intsz;
		size_t seg_left = intsz - seg_shift;
		size_t total_left = size - bitn;
		if (seg_left > total_left)
			seg_left = total_left;

		uint64_t seg = bits[seg_n];
		seg >>= seg_shift;
		if (!iset)
			seg = ~seg;
		//barrier already add -- msb `1`s
		else if (seg_shift) {
			//add a barrier `1`
			seg |= (uint64_t(1) << seg_left);
		}

		unsigned idx = __builtin_ffsll(seg);
		if (idx && idx <= seg_left) {
			return idx - 1;
		}

		return seg_left + __find_n(bitn + seg_left, iset);
	}

	size_t find_n1(size_t bitn) const
	{
		return bitn + __find_n(bitn, true);
	}

	size_t find_n0(size_t bitn) const
	{
		return bitn + __find_n(bitn, false);
	}
};

const uint64_t MY_BLOCK_SIZE = 512ULL;

struct AioChannel {
	bool writing;
	bool canceled;
	std::optional<int> err;
	uint64_t idx;
	uint64_t prev_idx;
	size_t len;
	void const *buffer;
	const uint64_t blocksz;
	const uint64_t discardsz;
	struct aiocb iocb;
	//std::vector<struct aiocb> iocb_lio;
	std::vector<std::pair<uint64_t, size_t> > discard_map;
	size_t sparse_discard_cnt;
	//size_t sparse_write_cnt;
	bitvector bitmap;

	inline AioChannel(void *_buff, uint64_t _bsz, size_t maxlen, uint64_t _discard_g)
		: canceled(false), idx(UINT64_MAX), prev_idx(UINT64_MAX), len(0), buffer(_buff), blocksz(_bsz),
		  discardsz(_discard_g)
	{
		memset(&iocb, 0, sizeof(iocb));
		iocb.aio_buf = (volatile void *)buffer;
		iocb.aio_nbytes = blocksz;
		iocb.aio_reqprio = 0;
		iocb.aio_sigevent.sigev_notify = SIGEV_SIGNAL;
		iocb.aio_sigevent.sigev_value.sival_ptr = this;
		//iocb_lio.resize(discardsz ? blocksz / discardsz / 2 : 0);
		discard_map.resize(discardsz ? blocksz * maxlen / discardsz / 2 : 0);
		DBG_AIO(1, "Channel @%p, buffer = %p, blocksz = %" PRIi64 ", discardsz = %" PRIi64 "\n", this, buffer,
			blocksz, discardsz);
	}

	inline void update_bitmap()
	{
		bitmap.resize_z(discardsz ? blocksz / discardsz * len : 0);
		if (!bitmap.size)
			return;
		size_t i = 0;
		for (unsigned char *disblock = (unsigned char *)buffer, *disend = disblock + blocksz * len;
		     disblock != disend; disblock += discardsz, ++i) {
			if (!is_mem_zero(disblock, discardsz))
				bitmap.set(i);
		}
		std::optional<bool> expected;
		//sparse_write_cnt = 0;
		sparse_discard_cnt = 0;
		DBG_AIO(4, "bitmap_dump: ");
		if (DBG_LEVEL_AIO > 3) {
			bitmap.print(stderr);
			DBG_AIO(-4, "\n");
		}
		DBG_AIO(3, "bitmap: ^%" PRIi64 "[", idx);
		for (size_t i = 0, j; i != bitmap.size; i = j) {
			if (!expected)
				expected.emplace(bitmap.test(i));
			if (*expected)
				j = bitmap.find_n0(i);
			else
				j = bitmap.find_n1(i);
			assert(j != i);
			assert(j <= bitmap.size);
			if (!*expected) {
				assert(discard_map.size() > sparse_discard_cnt);
				discard_map[sparse_discard_cnt++] =
					std::make_pair(blocksz * idx + discardsz * i, discardsz * (j - i));
				DBG_AIO(-3, "%zu-%zu, ", i, j);
			} /*else {
               assert(iocb_lio.size() > sparse_write_cnt);
               iocb_lio[sparse_write_cnt].aio_nbytes = discardsz * (j - i);
               iocb_lio[sparse_write_cnt].aio_offset = blocksz * idx + discardsz * i;
               iocb_lio[sparse_write_cnt++].aio_buf = (char*)buffer + discardsz * i;
               DBG_AIO(-3, "%" PRIi64 "^[%zu]...", blocksz / discardsz * idx + i, j - i);
            }*/
			*expected = !*expected;
		}
		DBG_AIO(-3, "]\n");
	}

	inline void check_invariant()
	{
		assert(iocb.aio_buf == buffer);
		assert(iocb.aio_nbytes == blocksz * len);
		assert((uint64_t)iocb.aio_offset == blocksz * idx);
		assert(!iocb.aio_reqprio);
		assert(iocb.aio_sigevent.sigev_notify == SIGEV_SIGNAL);
		assert(iocb.aio_sigevent.sigev_value.sival_ptr == this);
	}

	inline void set_check_inactive()
	{
		int status = aio_error(&iocb);
		if (status == EINPROGRESS)
			error(1, 0, "iosb should not be in progress");
		canceled = false;
		err.reset();
	}

	void set_idx(uint64_t _idx, size_t _len, uint64_t prev)
	{
		idx = _idx;
		len = _len;
		prev_idx = prev;
	}

	void prep_read(int fd, int signo)
	{
		iocb.aio_nbytes = blocksz * len;
		iocb.aio_offset = blocksz * idx;
		check_invariant();
		set_check_inactive();
		writing = false;
		iocb.aio_fildes = fd;
		iocb.aio_sigevent.sigev_signo = signo;
		if (aio_read(&iocb) < 0)
			error(1, errno, "aio_read failed");
	}
	void prep_write(int fd, int signo)
	{
		check_invariant();
		set_check_inactive();
		writing = true;
		iocb.aio_fildes = fd;
		iocb.aio_sigevent.sigev_signo = signo;
		if (aio_write(&iocb) < 0)
			error(1, errno, "aio_read failed");
	}
	int discard()
	{
		assert(writing);
		int rc = 0;
		for (size_t i = 0; i != sparse_discard_cnt && !rc; ++i) {
			DBG_AIO(3, "fallocate(PUNCH_HOLE, %" PRIi64 ", %zu)\n", discard_map[i].first,
				discard_map[i].second);
			rc = fallocate(iocb.aio_fildes, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
				       discard_map[i].first, discard_map[i].second);
		}
		return rc < 0 ? errno : 0;
	}
	void completion(int signo)
	{
		if (canceled || !!err)
			//status already collected
			return;
		if (signo != iocb.aio_sigevent.sigev_signo)
			error(1, 0, "aio completed but signo doesn't match");
		int status = aio_error(&iocb);
		if (status < 0)
			error(1, errno, "aio_error failed");
		if (status == EINPROGRESS)
			error(1, 0, "iocb should not be in progress");
		else {
			if (status == ECANCELED)
				canceled = true;
			ssize_t ret = aio_return(&iocb);
			if (ret < 0)
				err.emplace(errno);
			else if ((uint64_t)ret != blocksz * len)
				error(1, 0, "read/write doesn't give requested bytes (%" PRIi64 "/%" PRIi64 ")",
				      (uint64_t)ret, blocksz);
			else
				err.emplace(0);
		}
	}
};

struct AioCmpManager {
};

struct AioCopyManager {
	const uint64_t multiplier;
	uint64_t discard_gran;
	const unsigned channel;
	const unsigned chunk;
	const int fd_in;
	const int fd_out;
	const int signo;

	sigset_t oldmask;
	int sfd;
	int sfd_thr_discard;

	pthread_t thr_discard;

	void *buffers;
	std::map<uint64_t, std::unique_ptr<AioChannel> > channel_reading;
	std::map<uint64_t, std::unique_ptr<AioChannel> > channel_readdone;
	std::map<uint64_t, std::unique_ptr<AioChannel> > channel_writing;
	std::multimap<uint64_t, std::unique_ptr<AioChannel> > channel_free;

	static void *thread_entry_discard(void *arg)
	{
		((AioCopyManager *)arg)->thrDiscardLoop();
		return nullptr;
	}

	inline AioCopyManager(uint64_t _multi, unsigned _chan, unsigned _chunk, int _fin, int _fout, int _sig,
			      uint64_t _discard_gran = 0)
		: multiplier(_multi), discard_gran(_discard_gran), channel(_chan), chunk(_chunk), fd_in(_fin),
		  fd_out(_fout), signo(_sig)
	{
		long pagesz = sysconf(_SC_PAGESIZE);
		assert(pagesz > 0);
		if (multiplier * MY_BLOCK_SIZE * channel < (unsigned long)pagesz)
			error(2, 0, "buffer too small");

		if (discard_gran & (discard_gran - 1))
			error(2, 0, "discard_gran must be power of 2");

		if (discard_gran > multiplier)
			error(2, 0, "discard_gran must be less or equal to multiplier");

		struct stat statbuf;
		if (fstat(fd_out, &statbuf))
			error(1, errno, "fstat failed:");
		if (S_ISREG(statbuf.st_mode) && discard_gran) {
			DBG_AIO(1, "Using %" PRIi64 " bytes per FALLOC_FL_PUNCH_HOLE\n", discard_gran * MY_BLOCK_SIZE);
		} else {
			discard_gran = 0;
		}

		std::set<int> sigs_abort = { SIGHUP, SIGTERM, SIGINT };
		if (sigs_abort.count(signo) || sigs_abort.count(signo + 1))
			error(2, 0, "signo cannot collide with predefined system signals");
		sigset_t mask;

		sigemptyset(&mask);
		sigaddset(&mask, signo);
		sigaddset(&mask, signo + 1);
		for (auto &e : sigs_abort) {
			sigaddset(&mask, e);
		}
		if (pthread_sigmask(SIG_BLOCK, &mask, &oldmask) < 0)
			error(1, errno, "pthread_sigmask failed");

		sigdelset(&mask, signo + 1);
		if ((sfd = signalfd(-1, &mask, SFD_CLOEXEC)) < 0)
			error(1, errno, "signalfd failed");

		sigemptyset(&mask);
		sigaddset(&mask, signo + 1);
		if ((sfd_thr_discard = signalfd(-1, &mask, SFD_CLOEXEC)) < 0)
			error(1, errno, "signalfd failed");

		uint64_t buff_len = multiplier * chunk * MY_BLOCK_SIZE * channel;
		if ((buffers = mmap(NULL, buff_len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0)) ==
		    MAP_FAILED) {
			error(1, errno, "mmap failed");
		}
		madvise(buffers, buff_len, MADV_HUGEPAGE);
		for (unsigned i = 0; i != channel; ++i) {
			channel_free.emplace(UINT64_MAX,
					     std::make_unique<AioChannel>(
						     (char *)buffers + (uint64_t)i * MY_BLOCK_SIZE * multiplier * chunk,
						     MY_BLOCK_SIZE * multiplier, chunk, MY_BLOCK_SIZE * discard_gran));
		}
		int rc = pthread_create(&thr_discard, nullptr, thread_entry_discard, this);
		if (rc)
			error(1, rc, "pthread_create failed");
	}

	inline ~AioCopyManager()
	{
		sigqueue(getpid(), signo + 1, sigval{});
		pthread_join(thr_discard, nullptr);
		close(sfd);
		pthread_sigmask(SIG_SETMASK, &oldmask, NULL);
	}

	void complete_channel(AioChannel *chan)
	{
		DBG_AIO(2 - (chan->canceled || !!*chan->err),
			"Channel idx = %" PRIi64 "[%" PRIi64 "] %s finished%s%s%s\n", chan->idx, chan->len,
			chan->writing ? "(write)" : "(read)", chan->canceled ? ", CANCELLED" : "",
			(chan->err && *chan->err) ? ", ERROR: " : "",
			(chan->err && *chan->err) ? strerror(*chan->err) : "");
		if (!chan->canceled) {
			assert(chan->err);
		}
		if (chan->canceled || *chan->err) {
			DBG_AIO(1, "Channel idx = %" PRIi64 " %s dropped\n", chan->idx,
				chan->writing ? "(writing)" : "(reading)");
		}
		if (chan->writing) {
			auto it = channel_writing.find(chan->idx);
			assert(it != channel_writing.end());
			auto nh = channel_writing.extract(it++);
			if (chan->canceled || *chan->err) {
				nh.key() = UINT64_MAX;
			}
			channel_free.insert(std::move(nh));
		} else {
			auto it = channel_reading.find(chan->idx);
			assert(it != channel_reading.end());
			auto nh = channel_reading.extract(it++);
			if (chan->canceled || *chan->err) {
				//goes directly to freelist
				nh.key() = UINT64_MAX;
				channel_free.insert(std::move(nh));
			} else {
				channel_readdone.insert(std::move(nh));
			}
		}
	}

	void thrDiscardLoop()
	{
		for (;;) {
			struct signalfd_siginfo fdsi;
			ssize_t s = read(sfd_thr_discard, &fdsi, sizeof(fdsi));
			if (s != sizeof(fdsi))
				error(1, errno, "read_sfd(%d) failed", sfd);
			if (fdsi.ssi_signo != uint32_t(signo) + 1)
				error(1, 0, "unexpected signal received");
			if (fdsi.ssi_pid != (uint32_t)getpid()) {
				error(0, 0, "signal ignored: sent by %" PRIi32, fdsi.ssi_pid);
				continue;
			}
			DBG_AIO(2, "Signal %d: ssi_ptr = %p\n", fdsi.ssi_signo, (void *)fdsi.ssi_ptr);
			if ((void *)fdsi.ssi_ptr == MAP_FAILED) {
				//barrier, acknowledge it
				union sigval val {
				};
				val.sival_ptr = MAP_FAILED;
				sigqueue(getpid(), signo, val);
				continue;
			}
			auto chan = (AioChannel *)fdsi.ssi_ptr;
			if (!chan)
				break;
			chan->completion(fdsi.ssi_signo);
			if (!chan->canceled) {
				assert(!!chan->err);
				if (!*chan->err) {
					chan->update_bitmap();
					chan->err.emplace(chan->discard());
				}
			}
			union sigval val;
			val.sival_ptr = chan;
			sigqueue(getpid(), signo, val);
		}
	}

	AioChannel *checkSignal()
	{
		struct signalfd_siginfo fdsi;
		ssize_t s = read(sfd, &fdsi, sizeof(fdsi));
		if (s != sizeof(fdsi))
			error(1, errno, "read_sfd(%d) failed", sfd);
		switch (fdsi.ssi_signo) {
		case SIGINT:
		case SIGTERM:
		case SIGHUP:
			return nullptr;
		}
		if (fdsi.ssi_pid != (uint32_t)getpid()) {
			error(0, 0, "signal ignored: sent by %" PRIi32, fdsi.ssi_pid);
			return checkSignal();
		}
		DBG_AIO(2, "Signal %d: ssi_ptr = %p\n", fdsi.ssi_signo, (void *)fdsi.ssi_ptr);
		auto chan = (AioChannel *)fdsi.ssi_ptr;
		if (chan == MAP_FAILED)
			return chan;
		chan->completion(fdsi.ssi_signo);
		complete_channel(chan);
		return chan;
	}

	void stopAll()
	{
		DBG_AIO(1, "Stop All ...\n");
		if (channel_reading.size()) {
			DBG_AIO(1, "Cancelling all pending read\n");
			aio_cancel(fd_in, NULL);
		}
		if (channel_writing.size()) {
			DBG_AIO(1, "Cancelling all pending write\n");
			aio_cancel(fd_out, NULL);
		}
		while (channel_free.size() + channel_readdone.size() != channel) {
			checkSignal();
		}
		DBG_AIO(1, "All aio done\n");
		//drop done list
		while (channel_readdone.size()) {
			auto nh = channel_readdone.extract(channel_readdone.begin());
			DBG_AIO(1, "Channel idx = %" PRIi64 " finished read, dropped\n", nh.key());
			nh.key() = UINT64_MAX;
			channel_free.insert(std::move(nh));
		}
	}

	std::pair<uint64_t, uint64_t> runLoop(std::function<std::pair<uint64_t, uint64_t>(void)> &idxpool,
					      unsigned read_limit = 0, unsigned write_limit = 0)
	{
		assert(channel_free.size() == channel);
		uint64_t prev_idx = UINT64_MAX;
		auto free_it = channel_free.end();
		while ((free_it = channel_free.begin())->first != UINT64_MAX) {
			auto nh = channel_free.extract(free_it++);
			nh.key() = UINT64_MAX;
			//clear idx
			nh.mapped()->idx = UINT64_MAX;
			nh.mapped()->prev_idx = UINT64_MAX;
			channel_free.insert(std::move(nh));
		}
		uint64_t count = 0;
		for (;;) {
			bool work = false;
			free_it = channel_free.begin();
			if (free_it != channel_free.end() && (!read_limit || channel_reading.size() < read_limit)) {
				//try add channel
				auto idx_new = idxpool();
				if (idx_new.first != UINT64_MAX) {
					DBG_AIO(2, "Channel read:, idx = %" PRIi64 ", len = %zu\n", idx_new.first,
						(size_t)idx_new.second);
					auto nh = channel_free.extract(free_it++);
					//make sure we are not reusing failed io, we should have stopAll'ed
					assert(nh.key() == nh.mapped()->idx);
					assert(nh.key() != idx_new.first);
					count += nh.key() != UINT64_MAX;
					nh.key() = idx_new.first;
					nh.mapped()->set_idx(idx_new.first, idx_new.second, prev_idx);
					auto ret = channel_reading.insert(std::move(nh));
					assert(ret.inserted);
					ret.position->second->prep_read(fd_in, signo);
					work = true;
				}
				prev_idx = idx_new.first;
			}
			auto done_it = channel_readdone.begin();
			if (done_it != channel_readdone.end() &&
			    (!write_limit || channel_writing.size() < write_limit)) {
				//try reap channel
				auto nh = channel_readdone.extract(done_it++);
				DBG_AIO(2, "Channel write: idx = %" PRIi64 ", len = %zu", nh.key(), nh.mapped()->len);
				auto ret = channel_writing.insert(std::move(nh));
				assert(ret.inserted);
				if (discard_gran) {
					DBG_AIO(-2, ", discard(%" PRIi64 ")\n", ret.position->second->discardsz);
				} else {
					DBG_AIO(-2, "\n");
				}
				ret.position->second->prep_write(fd_out, signo + (!!discard_gran));
				work = true;
			}
			if (!work) {
				if (channel_free.size() == channel)
					break;
				auto *chan = checkSignal();
				assert(chan != MAP_FAILED);
				if (!chan || chan->canceled || *chan->err) {
					stopAll();
					break;
				}
			}
		}
		//make sure all channels are done
		assert(channel_free.size() == channel);

		//report statistics
		auto range = channel_free.equal_range(UINT64_MAX);
		if (DBG_LEVEL_AIO > 0) {
			DBG_AIO(1, "All Finished: Last Successful = ");
			for (auto p = channel_free.begin(); p != range.first; ++p) {
				DBG_AIO(-1, "%" PRIi64 "[%" PRIi64 "], ", p->first, p->second->len);
			}
			DBG_AIO(-1, "Last Failed = ");
			for (auto p = range.first; p != range.second; ++p) {
				DBG_AIO(-1, "%" PRIi64 "[%" PRIi64 "]/%" PRIi64 ", ", p->second->idx, p->second->len,
					p->second->prev_idx);
			}
			DBG_AIO(-1, "\n");
		}
		std::optional<uint64_t> last_success;
		uint64_t failed = 0;
		for (auto i = range.first; i != range.second; ++i) {
			if (i->second->idx != UINT64_MAX) {
				++failed;
				if (!last_success)
					last_success.emplace(i->second->prev_idx);
				else
					last_success.emplace(std::min(i->second->prev_idx, *last_success));
			} else {
				assert(i->second->prev_idx == UINT64_MAX);
			}
		}
		if (!last_success) {
			assert(!failed);
			auto j = range.first;
			if (j != channel_free.begin())
				last_success.emplace((--j)->first);
			else
				//nothing was done
				last_success.emplace(UINT64_MAX);
		}

		return std::make_pair(*last_success, failed);
	}
};
