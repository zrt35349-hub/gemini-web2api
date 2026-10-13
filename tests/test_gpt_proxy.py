"""GPT proxy tests: session wiring, hot-swap and config normalization."""

import unittest
from unittest.mock import patch

from chatgptwebapp2api.chatgpt import ChatGPTClient
from chatgptwebapp2api.config import Config


class ProxySessionTests(unittest.TestCase):
    def test_proxy_is_passed_to_session(self):
        with patch("chatgptwebapp2api.chatgpt.Session") as mock_session:
            client = ChatGPTClient("oai-did=x", proxy="http://127.0.0.1:7890")
            _ = client.session
            kwargs = mock_session.call_args[1]
            self.assertEqual(kwargs["proxies"],
                             {"http": "http://127.0.0.1:7890",
                              "https": "http://127.0.0.1:7890"})

    def test_no_proxy_means_direct_session(self):
        with patch("chatgptwebapp2api.chatgpt.Session") as mock_session:
            client = ChatGPTClient("oai-did=x", proxy=None)
            _ = client.session
            self.assertNotIn("proxies", mock_session.call_args[1])

    def test_update_proxy_hot_swaps_sessions(self):
        with patch("chatgptwebapp2api.chatgpt.Session") as mock_session:
            client = ChatGPTClient("oai-did=x", proxy=None)
            _ = client.session
            self.assertTrue(client.update_proxy("http://127.0.0.1:7890"))
            self.assertEqual(client._proxy, "http://127.0.0.1:7890")
            _ = client.session
            self.assertEqual(mock_session.call_args[1]["proxies"]["http"],
                             "http://127.0.0.1:7890")
            self.assertFalse(client.update_proxy("http://127.0.0.1:7890"))
            self.assertTrue(client.update_proxy(""))
            self.assertIsNone(client._proxy)


class ProxyConfigTests(unittest.TestCase):
    def test_empty_proxy_normalizes_to_none(self):
        cfg = Config(proxy="")
        cfg.__post_init__()
        self.assertIsNone(cfg.proxy)

    def test_proxy_is_stripped(self):
        cfg = Config(proxy="http://127.0.0.1:7890 ")
        cfg.__post_init__()
        self.assertEqual(cfg.proxy, "http://127.0.0.1:7890")


if __name__ == "__main__":
    unittest.main()
