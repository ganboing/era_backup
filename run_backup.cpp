#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <cstdio>
#include <string_view>
#include <functional>
#include <iterator>
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <fcntl.h>
#include <error.h>
#include <errno.h>
#include <getopt.h>
#include <rapidxml.hpp>
#include <rapidxml_utils.hpp>
#include "aio_manager.cpp"

static uint64_t get_sz(int fd)
{
	uint64_t ret;
	int err = ioctl(fd, BLKGETSIZE64, &ret);
	if (!err)
		return ret;
	struct stat buf;
	err = fstat(fd, &buf);
	if (err)
		error(1, errno, "fstat failed: ");
	return buf.st_size;
}

#if 0

#ifndef __NR_copy_file_range
#if defined(__x86_64__)
#define __NR_copy_file_range 326
#endif
#endif

#if __GLIBC_PREREQ(2, 27)
#else
static loff_t
copy_file_range(int fd_in, loff_t *off_in, int fd_out,
                      loff_t *off_out, size_t len, unsigned int flags)
{
       return syscall(__NR_copy_file_range, fd_in, off_in, fd_out,
                                off_out, len, flags);
}
#endif

static void copy_range(int fd_in, int fd_out, loff_t offset, size_t len)
{
   loff_t off_in = offset, off_out = offset;
   ssize_t ret = copy_file_range(fd_in, &off_in, fd_out, &off_out, len, 0);
   if(ret < 0 || (size_t)ret != len)
      error(1, errno, "copy_range(%d, %d, %lu, %lu) failed",
            fd_in, fd_out, (unsigned long)offset, (unsigned long)len);
}

#endif

static option long_opts[] = { { "help", no_argument, nullptr, 'h' },
			      { "format", required_argument, nullptr, 'm' },
			      { "dry", no_argument, nullptr, 'd' },
			      { "channel", required_argument, nullptr, 'c' },
			      { "chunk", required_argument, nullptr, 'C' },
			      { "discard", required_argument, nullptr, 'D' },
			      { "restart", required_argument, nullptr, 's' },
			      { "progress", no_argument, nullptr, 'p' },
			      { "rlimit", required_argument, nullptr, 'r' },
			      { "wlimit", required_argument, nullptr, 'w' },
			      { nullptr, 0, nullptr, 0 } };

static void help(const char *argv0)
{
	fprintf(stderr,
		"Usage: \n"
		"\t%s [OPTIONS...] <in> <out> <blocksz(in 512b sectors)>\n"
		"\n"
		"\t-h, --help      Display help message\n"
		"\t-m, --format    Metadata format: era|plain\n"
		"\t-d, --dry       Dry run\n"
		"\t-s, --restart   Restart at N'th block\n"
		"\t-p, --progress  Report progress\n"
		"\t-c, --channel   Number of channels\n"
		"\t-C, --chunk     Max contiguous blocks to read/write\n"
		"\t-D, --discard   Granularity of FALLOC_FL_PUNCH_HOLE (in 512b sectors)\n"
		"\t-r, --rlimit    Limit the number of concurrent read channels\n"
		"\t-w, --wlimit    Limit the number of concurrent write channels\n"
		"\n",
		argv0);
}

static std::pair<uint64_t, uint64_t> parse_era_entry(rapidxml::xml_node<> *node)
{
	using namespace std::literals;
	std::string_view type = node->name();
	if (type == "block"sv) {
		auto b = node->first_attribute("block");
		if (!b)
			error(2, 0, "invalid era format: malformed block");
		uint64_t bn = strtoull(b->value(), nullptr, 10);
		return std::make_pair(bn, bn + 1);
	} else if (type == "range"sv) {
		auto l = node->first_attribute("begin");
		auto r = node->first_attribute("end");
		if (!l || !r) {
			error(2, 0, "invalid era format: malformed range");
		}
		uint64_t ln = strtoull(l->value(), nullptr, 10);
		uint64_t rn = strtoull(r->value(), nullptr, 10);
		if (ln > rn)
			error(2, 0, "invalid era format: begin > end");
		return std::make_pair(ln, rn);
	} else
		error(2, 0, "invalid era format: encountered node: %s", node->name());
	__builtin_unreachable();
}

int main(int argc, char **argv)
{
	using namespace std::literals;
	int opt_idx = 0;
	int c;
	const char *format = "plain";
	unsigned channels = 1;
	unsigned limit_r = 0, limit_w = 0;
	uint64_t restart = UINT64_MAX;
	bool dry = false;
	bool progress = false;
	unsigned max_chunk = 1;
	uint64_t discard_gran = 0;
	while ((c = getopt_long(argc, argv, "hdpm:c:C:D:s:r:w:", long_opts, &opt_idx)) != -1) {
		switch (c) {
		case 'h':
			help(argv[0]);
			return 0;
		case 'm':
			format = optarg;
			break;
		case 'c':
			channels = strtoul(optarg, nullptr, 10);
			break;
		case 'C':
			max_chunk = strtoul(optarg, nullptr, 10);
			break;
		case 's':
			restart = strtoull(optarg, nullptr, 10);
			break;
		case 'r':
			limit_r = strtoul(optarg, nullptr, 10);
			break;
		case 'w':
			limit_w = strtoul(optarg, nullptr, 10);
			break;
		case 'd':
			dry = true;
			break;
		case 'D':
			discard_gran = strtoull(optarg, nullptr, 10);
			break;
		case 'p':
			progress = true;
			break;
		default:
			return -1;
		}
	}
	if (argc - optind < 3) {
		help(argv[0]);
		error(1, 0, "insufficient arguments!");
	}
	char **opts = argv + optind;
	uint64_t multiplier = strtoull(opts[2], nullptr, 10);
	if (multiplier & (multiplier - 1))
		error(2, 0, "multiplier must be power of 2");

	std::string_view format_sv = format;

	std::function<std::pair<uint64_t, uint64_t>(void)> idx_pool;
	if (format_sv == "era"sv) {
		std::cin >> std::noskipws;
		std::istream_iterator<char> it(std::cin);
		std::istream_iterator<char> end;
		auto strf = std::make_shared<std::string>(it, end);
		auto doc = std::make_shared<rapidxml::xml_document<> >();
		doc->parse<0>(&strf->front());
		auto *blocks = doc->first_node("blocks");
		if (!blocks)
			error(2, 0, "unable to find blocks section in era dump");
		auto *node = blocks->first_node();
		uint64_t l = 0, r = 0;
		if (node) {
			auto range = parse_era_entry(node);
			l = range.first;
			r = range.second;
		}
		uint64_t cnt = 0;
		uint64_t total = 0;
		for (;;) {
			idx_pool = [strf, doc, blocks, node, l, r, max_chunk, cnt, total]() mutable {
				if (l == r) {
					if (node)
						node = node->next_sibling();
				}
				if (!node)
					return std::pair<uint64_t, uint64_t>(UINT64_MAX, 0);
				if (l == r) {
					auto range = parse_era_entry(node);
					l = range.first;
					r = range.second;
				}
				uint64_t chunk = (r - l > max_chunk) ? max_chunk : r - l;
				if (total) {
					if (cnt * 20 / total < (cnt + chunk) * 20 / total) {
						unsigned perc = (cnt + chunk) * 100 / total;
						fprintf(stderr, "%d%%...%s", perc, perc == 100 ? "\n" : "");
					}
					cnt += chunk;
				}
				uint64_t ret = l;
				l += chunk;
				return std::pair<uint64_t, uint64_t>(ret, chunk);
			};
			if (progress) {
				auto ret = idx_pool();
				while (ret.first != UINT64_MAX) {
					total += ret.second;
					ret = idx_pool();
				}
				fprintf(stderr, "Total %" PRIi64 " blocks\n", total);
				progress = false;
			} else
				break;
		};
	} else if (format_sv == "plain"sv) {
		idx_pool = []() {
			uint64_t block;
			if (std::cin >> block)
				return std::pair<uint64_t, uint64_t>(block, 1);
			return std::pair<uint64_t, uint64_t>(UINT64_MAX, 0);
		};
	} else {
		error(1, 0, "unsupported format %s", format);
	}
	if (restart != UINT64_MAX) {
		while (idx_pool().first != restart)
			;
	}
	if (dry) {
		std::pair<uint64_t, uint64_t> idx;
		printf("Dry run: ");
		while ((idx = idx_pool()).first != UINT64_MAX) {
			printf("%" PRIi64 "[%" PRIi64 "] ", idx.first, idx.second);
		}
		putc('\n', stdout);
		return 0;
	}

	ManagedFd fd_in = open(opts[0], O_RDONLY | O_DIRECT);
	if (fd_in.fd < 0)
		error(1, errno, "open(%s, O_RDONLY | O_DIRECT) failed", opts[0]);
	ManagedFd fd_out = open(opts[1], O_WRONLY | O_DIRECT);
	if (fd_out.fd < 0)
		error(1, errno, "open(%s, O_WRONLY | O_DIRECT) failed", opts[1]);
	if (get_sz(fd_in.fd) != get_sz(fd_out.fd))
		error(2, 0, "Size does not match!");

	AioCopyManager aio(multiplier, channels, max_chunk, fd_in.fd, fd_out.fd, SIGRTMIN, discard_gran);
	auto ret = aio.runLoop(idx_pool, limit_r, limit_w);

	return (ret.second || idx_pool().first != UINT64_MAX) ? 1 : 0;
}
