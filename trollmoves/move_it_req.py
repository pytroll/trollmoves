"""Send a request to a move_it server and wait for a reply.

Request could fx. be a "ping" or "info"
"""
import argparse
import time

import zmq
from posttroll.message import Message

REQUEST_TIMEOUT = 4500
REQUEST_RETRIES = 3
DEFAULT_SERVER = "tcp://localhost:9092"


def parse_args():
    """Parser commandline arguments."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ping", action="store_true", help="send ping request")
    group.add_argument("--info", metavar="<topic>", help="send a info requst on topic")
    parser.add_argument("--spam", metavar="<sleep-time>", type=float, default=None,
                        help="continue send requests with specified sleep time")
    parser.add_argument("--extra", metavar="<key:val>",
                        help="extra key/value pairs to be send with request, seperated by ','")
    parser.add_argument("-v", "--verbose", action="store_true", help="print more information")
    parser.add_argument("server", nargs="?", default=DEFAULT_SERVER,
                        help="server endpoint (default: %s)" % DEFAULT_SERVER)

    args = parser.parse_args()

    if not args.server.startswith("tcp://"):
        args.server = "tcp://" + args.server

    return args


def get_request_data(args):
    """Get request data."""
    import ast
    req_data = {}
    if args.extra:
        for k_v in args.extra.split(","):
            k, v = k_v.split(":")
            try:
                req_data[k] = ast.literal_eval(v)
            except Exception:
                req_data[k] = v

    return req_data


def info_formatter(args, msg):
    str_ = msg.head
    d_ = {}
    for k, v in msg.data.items():
        if k == "files":
            d_["file_count"] = len(v)
        else:
            d_[str(k)] = v
    str_ += " " + str(d_)
    if args.verbose:
        for f in msg.data["files"]:
            str_ += "\n" + f
    return str_


def run(args):
    """Run the requester."""
    if args.ping:
        req_type = "ping"
        req_topic = "ping/pong"
        rep_formatter = str
    elif args.info is not None:
        req_type = "info"
        req_topic = args.info
        rep_formatter = info_formatter

    req_data = get_request_data(args)

    context = zmq.Context(1)

    print("Connecting to '%s' ..." % args.server)
    client = context.socket(zmq.REQ)
    client.connect(args.server)

    poll = zmq.Poller()
    poll.register(client, zmq.POLLIN)

    retries_left = REQUEST_RETRIES
    try:

        while retries_left:
            request = str(Message(req_topic, req_type, req_data))
            print("Sending (%s)" % request)
            client.send(request)

            expect_reply = True
            while expect_reply:
                socks = dict(poll.poll(REQUEST_TIMEOUT))
                if socks.get(client) == zmq.POLLIN:
                    reply = client.recv()
                    if not reply:
                        break
                    reply = Message(rawstr=reply)
                    print("Server replied: %s" % rep_formatter(reply))
                    retries_left = REQUEST_RETRIES
                    expect_reply = False

                else:
                    print("No response from server, retrying ...")
                    # Socket is confused. Close and remove it.
                    client.setsockopt(zmq.LINGER, 0)
                    client.close()
                    poll.unregister(client)
                    retries_left -= 1
                    if retries_left == 0:
                        print("Server seems to be offline, abandoning")
                        break
                    print("Reconnecting and resending (%s)" % request)
                    # Create new connection
                    client = context.socket(zmq.REQ)
                    client.connect(args.server)
                    poll.register(client, zmq.POLLIN)
                    client.send(request)
            if args.spam is not None:
                time.sleep(args.spam)
            else:
                break
    except KeyboardInterrupt:
        pass
    finally:
        context.term()


def main():
    """Run the script."""
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
