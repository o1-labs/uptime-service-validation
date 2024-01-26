import logging
import sys
from uptime_service_validation.coordinator.server import try_get_hostname_ip


def test_try_get_hostname_ip():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    assert try_get_hostname_ip("localhost", logging) == "127.0.0.1"
    assert (
        try_get_hostname_ip("wronglocalhost", logging, max_retries=3, initial_wait=0.1)
        == "wronglocalhost"
    )
