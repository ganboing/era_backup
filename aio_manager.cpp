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

static void DBG_AIO(int level, const char *fmt, ...)
	__attribute__((format(printf, 2, 3)));

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

const uint64_t MY_BLOCK_SIZE = 512ULL;

struct AioCopyManager {
	const uint64_t multiplier;
	const unsigned channel;
	const int fd_in;
	const int fd_out;
	const int sigr;
	const int sigw;

	sigset_t oldmask;
	int sfd;

	struct AioChannel {
		bool writing;
		bool canceled;
		int err;
		uint64_t idx;
		uint64_t prev_idx;
		void const *buffer;
		const uint64_t blocksz;
		struct aiocb iocb;

		inline AioChannel(void *_buff, uint64_t _bsz)
			: canceled(false), err(0), idx(UINT64_MAX),
			  prev_idx(UINT64_MAX), buffer(_buff), blocksz(_bsz)
		{
			memset(&iocb, 0, sizeof(iocb));
			iocb.aio_buf = (volatile void *)buffer;
			iocb.aio_nbytes = blocksz;
			iocb.aio_reqprio = 0;
			iocb.aio_sigevent.sigev_notify = SIGEV_SIGNAL;
			iocb.aio_sigevent.sigev_value.sival_ptr = this;
		}

		inline void check_invariant()
		{
			assert(iocb.aio_buf == buffer);
			assert(iocb.aio_nbytes == blocksz);
			assert(!iocb.aio_reqprio);
			assert(iocb.aio_sigevent.sigev_notify == SIGEV_SIGNAL);
			assert(iocb.aio_sigevent.sigev_value.sival_ptr == this);
		}

		inline void check_inactive()
		{
			int status = aio_error(&iocb);
			if (status == EINPROGRESS)
				error(1, 0, "iosb should not be in progress");
		}

		void prep_read(int fd, int signo)
		{
			err = 0;
			canceled = false;
			check_invariant();
			check_inactive();
			writing = false;
			iocb.aio_fildes = fd;
			iocb.aio_offset = idx * blocksz;
			iocb.aio_sigevent.sigev_signo = signo;
			if (aio_read(&iocb) < 0)
				error(1, errno, "aio_read failed");
		}
		void prep_write(int fd, int signo)
		{
			err = 0;
			canceled = false;
			check_invariant();
			check_inactive();
			writing = true;
			iocb.aio_fildes = fd;
			assert((uint64_t)iocb.aio_offset == idx * blocksz);
			iocb.aio_sigevent.sigev_signo = signo;
			if (aio_write(&iocb) < 0)
				error(1, errno, "aio_read failed");
		}
		void completion(int signo)
		{
			if (signo != iocb.aio_sigevent.sigev_signo)
				error(1, 0,
				      "aio completed but signo doesn't match");
			assert(!err && !canceled);
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
					err = errno;
				else if ((uint64_t)ret != blocksz)
					error(1, 0,
					      "read/write doesn't give requested bytes (%" PRIi64
					      "/%" PRIi64 ")",
					      (uint64_t)ret, blocksz);
			}
		}
	};
	void *buffers;
	std::map<uint64_t, std::unique_ptr<AioChannel> > channel_reading;
	std::map<uint64_t, std::unique_ptr<AioChannel> > channel_readdone;
	std::map<uint64_t, std::unique_ptr<AioChannel> > channel_writing;
	std::multimap<uint64_t, std::unique_ptr<AioChannel> > channel_free;

	inline AioCopyManager(uint64_t _multi, unsigned _chan, int _fin,
			      int _fout, int _sigr, int _sigw)
		: multiplier(_multi), channel(_chan), fd_in(_fin),
		  fd_out(_fout), sigr(_sigr), sigw(_sigw)
	{
		long pagesz = sysconf(_SC_PAGESIZE);
		assert(pagesz > 0);
		if (multiplier * MY_BLOCK_SIZE * channel <
		    (unsigned long)pagesz)
			error(2, 0, "buffer too small");

		std::set<int> sigs_abort = { SIGHUP, SIGTERM, SIGINT };
		if (sigs_abort.count(sigr) || sigs_abort.count(sigw))
			error(2, 0,
			      "signo cannot collide with predefined system signals");
		sigset_t mask;

		sigemptyset(&mask);
		sigaddset(&mask, sigr);
		sigaddset(&mask, sigw);
		for (auto &e : sigs_abort) {
			sigaddset(&mask, e);
		}
		if (sigprocmask(SIG_BLOCK, &mask, &oldmask) < 0)
			error(1, errno, "sigprocmask failed");

		if ((sfd = signalfd(-1, &mask, SFD_CLOEXEC)) < 0)
			error(1, errno, "signalfd failed");

		if ((buffers = mmap(NULL, multiplier * MY_BLOCK_SIZE * channel,
				    PROT_READ | PROT_WRITE,
				    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0)) ==
		    MAP_FAILED) {
			error(1, errno, "mmap failed");
		}
		for (unsigned i = 0; i != channel; ++i) {
			channel_free.emplace(
				UINT64_MAX,
				std::make_unique<AioChannel>(
					(char *)buffers +
						(uint64_t)i * MY_BLOCK_SIZE *
							multiplier,
					MY_BLOCK_SIZE * multiplier));
		}
	}

	inline ~AioCopyManager()
	{
		close(sfd);
		sigprocmask(SIG_SETMASK, &oldmask, NULL);
	}

	void complete_channel(AioChannel *chan)
	{
		DBG_AIO(2 - (chan->err || chan->canceled),
			"Channel idx = %" PRIi64 " %s finished%s%s%s\n",
			chan->idx, chan->writing ? "(write)" : "(read)",
			chan->canceled ? ", CANCELLED" : "",
			chan->err ? ", ERROR: " : "",
			chan->err ? strerror(chan->err) : "");
		if (chan->err || chan->canceled) {
			DBG_AIO(1, "Channel idx = %" PRIi64 " %s dropped\n",
				chan->idx,
				chan->writing ? "(writing)" : "(reading)");
		}
		if (chan->writing) {
			auto it = channel_writing.find(chan->idx);
			assert(it != channel_writing.end());
			auto nh = channel_writing.extract(it++);
			if (chan->err || chan->canceled) {
				nh.key() = UINT64_MAX;
			}
			channel_free.insert(std::move(nh));
		} else {
			auto it = channel_reading.find(chan->idx);
			assert(it != channel_reading.end());
			auto nh = channel_reading.extract(it++);
			if (chan->err || chan->canceled) {
				//goes directly to freelist
				nh.key() = UINT64_MAX;
				channel_free.insert(std::move(nh));
			} else {
				channel_readdone.insert(std::move(nh));
			}
		}
	}

	bool checkSignal()
	{
		struct signalfd_siginfo fdsi;
		ssize_t s = read(sfd, &fdsi, sizeof(fdsi));
		if (s != sizeof(fdsi))
			error(1, errno, "read_sfd(%d) failed", sfd);
		switch (fdsi.ssi_signo) {
		case SIGINT:
		case SIGTERM:
		case SIGHUP:
			return false;
		}
		if (fdsi.ssi_pid != (uint32_t)getpid()) {
			error(0, 0, "signal ignored: sent by %" PRIi32,
			      fdsi.ssi_pid);
			return true;
		}
		DBG_AIO(2, "Signal %d: ssi_ptr = %p\n", fdsi.ssi_signo,
			(void *)fdsi.ssi_ptr);
		auto chan = (AioChannel *)fdsi.ssi_ptr;
		chan->completion(fdsi.ssi_signo);
		complete_channel(chan);
		return !chan->err && !chan->canceled;
	}

	void stopAll()
	{
		DBG_AIO(2, "Stop All ...\n");
		if (channel_reading.size()) {
			DBG_AIO(2, "Cancelling all pending read\n");
			aio_cancel(fd_in, NULL);
		}
		if (channel_writing.size()) {
			DBG_AIO(2, "Cancelling all pending write\n");
			aio_cancel(fd_out, NULL);
		}
		while (channel_free.size() + channel_readdone.size() !=
		       channel) {
			checkSignal();
		}
		//drop done list
		while (channel_readdone.size()) {
			auto nh = channel_readdone.extract(
				channel_readdone.begin());
			DBG_AIO(1,
				"Channel idx = %" PRIi64
				" finished read, dropped\n",
				nh.key());
			nh.key() = UINT64_MAX;
			channel_free.insert(std::move(nh));
		}
	}

	std::pair<uint64_t, uint64_t>
	runLoop(std::function<uint64_t(void)> &&idxpool,
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
			if (free_it != channel_free.end() &&
			    (!read_limit ||
			     channel_reading.size() < read_limit)) {
				//try add channel
				uint64_t idx = idxpool();
				if (idx != UINT64_MAX) {
					DBG_AIO(2,
						"Channel added, idx = %" PRIi64
						", count = %" PRIi64 "\n",
						idx, count);
					auto nh =
						channel_free.extract(free_it++);
					assert(nh.key() == nh.mapped()->idx);
					assert(nh.key() != idx);
					count += nh.key() != UINT64_MAX;
					nh.key() = nh.mapped()->idx = idx;
					nh.mapped()->prev_idx = prev_idx;
					auto ret = channel_reading.insert(
						std::move(nh));
					assert(ret.inserted);
					ret.position->second->prep_read(fd_in,
									sigr);
					work = true;
				}
				prev_idx = idx;
			}
			auto done_it = channel_readdone.begin();
			if (done_it != channel_readdone.end() &&
			    (!write_limit ||
			     channel_writing.size() < write_limit)) {
				//try reap channel
				auto nh = channel_readdone.extract(done_it++);
				DBG_AIO(2,
					"Channel switched to writing, idx = %" PRIi64
					"\n",
					nh.key());
				auto ret =
					channel_writing.insert(std::move(nh));
				assert(ret.inserted);
				ret.position->second->prep_write(fd_out, sigw);
				work = true;
			}
			if (!work) {
				if (channel_free.size() == channel)
					break;
				if (!checkSignal()) {
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
			for (auto p = channel_free.begin(); p != range.first;
			     ++p) {
				DBG_AIO(-1, "%" PRIi64 ", ", p->first);
			}
			DBG_AIO(-1, "Last Failed = ");
			for (auto p = range.first; p != range.second; ++p) {
				DBG_AIO(-1, "%" PRIi64 "/%" PRIi64 ", ",
					p->second->idx, p->second->prev_idx);
			}
			DBG_AIO(-1, "\n");
		}
		std::optional<uint64_t> last_success;
		for (auto i = range.first; i != range.second; ++i) {
			if (i->second->idx != UINT64_MAX) {
				if (!last_success)
					last_success.emplace(
						i->second->prev_idx);
				else
					last_success.emplace(
						std::min(i->second->prev_idx,
							 *last_success));
			} else {
				assert(i->second->prev_idx == UINT64_MAX);
			}
		}
		if (!last_success) {
			auto j = range.first;
			assert(j != channel_free.begin());
			last_success.emplace((--j)->first);
		}

		return std::make_pair(*last_success,
				      count + channel_free.size() -
					      std::distance(range.first,
							    range.second));
	}
};
