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

static unsigned long get_sz(int fd)
{
	unsigned long ret;
	int err = ioctl(fd, BLKGETSIZE64, &ret);
	if (err)
		error(1, errno, "ioctl(%d, BLKGETSIZE64) failed", fd);
	return ret;
}

static option long_opts[] = { { "help", no_argument, nullptr, 'h' },
			      { "format", required_argument, nullptr, 'm' },
			      { "dry", no_argument, nullptr, 'd' },
			      { "channel", required_argument, nullptr, 'c' },
			      { "restart", required_argument, nullptr, 's' },
			      { "rlimit", required_argument, nullptr, 'r' },
			      { "wlimit", required_argument, nullptr, 'w' },
			      { nullptr, 0, nullptr, 0 } };

static void help(const char *argv0)
{
	fprintf(stderr,
		"Usage: \n"
		"\t%s [OPTIONS...] <in> <out> <blocksz(in 512b sectors)>\n"
		"\n"
		"\t-h, --help     Display help message\n"
		"\t-m, --format   Metadata format: era|plain\n"
		"\t-d, --dry      Dry run\n"
		"\t-s, --restart  Restart at N'th block\n"
		"\t-c, --channel  Number of channels\n"
		"\t-r, --rlimit   Limit the number of concurrent read channels\n"
		"\t-w, --wlimit   Limit the number of concurrent write channels\n",
		argv0);
}

static std::pair<uint64_t, uint64_t> parse_era_entry(rapidxml::xml_node<> *node)
{
	using namespace std::literals;
	std::string_view type = node->name();
	if (type == "block" sv) {
		auto b = node->first_attribute("block");
		if (!b)
			error(2, 0, "invalid era format: malformed block");
		uint64_t bn = strtoull(b->value(), nullptr, 10);
		return std::make_pair(bn, bn + 1);
	} else if (type == "range" sv) {
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
		error(2, 0, "invalid era format: encountered node: %s",
		      node->name());
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
	while ((c = getopt_long(argc, argv, "hdm:c:s:r:w:", long_opts,
				&opt_idx)) != -1) {
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
		default:
			return -1;
		}
	}
	if (argc - optind < 3) {
		help(argv[0]);
		error(1, 0, "insufficient arguments!");
	}

	std::string_view format_sv = format;

	std::function<uint64_t(void)> idx_pool;
	if (format_sv == "era" sv) {
		std::cin >> std::noskipws;
		std::istream_iterator<char> it(std::cin);
		std::istream_iterator<char> end;
		auto strf = std::make_shared<std::string>(it, end);
		auto doc = std::make_shared<rapidxml::xml_document<> >();
		doc->parse<0>(&strf->front());
		auto *blocks = doc->first_node("blocks");
		if (!blocks)
			error(2, 0,
			      "unable to find blocks section in era dump");
		auto *node = blocks->first_node();
		uint64_t l = 0, r = 0;
		if (node) {
			auto range = parse_era_entry(node);
			l = range.first;
			r = range.second;
		}
		idx_pool = [strf, doc, blocks, node, l, r]() mutable {
			if (l == r) {
				if (node)
					node = node->next_sibling();
			}
			if (!node)
				return UINT64_MAX;
			if (l == r) {
				auto range = parse_era_entry(node);
				l = range.first;
				r = range.second;
			}
			return l++;
		};
	} else if (format_sv == "plain" sv) {
		idx_pool = []() {
			uint64_t block;
			if (std::cin >> block)
				return block;
			return UINT64_MAX;
		};
	} else {
		error(1, 0, "unsupported format %s", format);
	}
	if (restart != UINT64_MAX) {
		while (idx_pool() != restart)
			;
	}
	if (dry) {
		uint64_t idx;
		printf("Dry run: ");
		while ((idx = idx_pool()) != UINT64_MAX) {
			printf("%" PRIi64 " ", idx);
		}
		putc('\n', stdout);
		return 0;
	}

	char **opts = argv + optind;
	ManagedFd fd_in = open(opts[0], O_RDONLY | O_DIRECT);
	if (fd_in.fd < 0)
		error(1, errno, "open(%s, O_RDONLY | O_DIRECT) failed",
		      opts[0]);
	ManagedFd fd_out = open(opts[1], O_WRONLY | O_DIRECT);
	if (fd_out.fd < 0)
		error(1, errno, "open(%s, O_WRONLY | O_DIRECT) failed",
		      opts[1]);
	if (get_sz(fd_in.fd) != get_sz(fd_out.fd))
		error(2, 0, "Size does not match!");
	uint64_t multiplier = strtoull(opts[2], nullptr, 10);
	if (multiplier & (multiplier - 1))
		error(2, 0, "multipler must be power of 2");

	AioCopyManager aio(multiplier, channels, fd_in.fd, fd_out.fd, SIGRTMIN,
			   SIGRTMIN + 1);
	aio.runLoop(std::move(idx_pool), limit_r, limit_w);
}
