import logging
import os
import sys
from uptime_service_validation.coordinator.server import (
    bool_env_var_set,
    try_get_hostname_ip,
)


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


def test_bool_env_var_set():
    os.environ["TEST_VAR"] = "True"
    assert bool_env_var_set("TEST_VAR") == True
    os.environ["TEST_VAR"] = "1"
    assert bool_env_var_set("TEST_VAR") == True
    os.environ["TEST_VAR"] = "False"
    assert bool_env_var_set("TEST_VAR") == False
    os.environ["TEST_VAR"] = "0"
    assert bool_env_var_set("TEST_VAR") == False

    assert bool_env_var_set("TEST_VAR_THAT_IS_NOT_SET") == False
