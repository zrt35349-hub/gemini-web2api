import unittest

from chatgptwebapp2api.chatgpt import ChatGPTClient, ChatGPTError
from chatgptwebapp2api.server import (
    PromptTooLong,
    messages_to_prompt,
    start_conversation_with_compaction,
)


class UpstreamErrorTests(unittest.TestCase):
    def test_upstream_413_remains_a_client_error(self):
        class Response:
            status_code = 413
            text = ""

        client = ChatGPTClient("cookie")
        with self.assertRaises(ChatGPTError) as error:
            client._raise_for_status(Response(), "POST conversation")

        self.assertEqual(error.exception.status, 413)
        self.assertIn("request too large", str(error.exception))


class MessagesToPromptTests(unittest.TestCase):
    def test_short_history_is_unchanged(self):
        messages = [
            {"role": "user", "content": "First"},
            {"role": "assistant", "content": "Reply"},
            {"role": "user", "content": "Latest"},
        ]
        self.assertEqual(
            messages_to_prompt(messages, max_bytes=1000),
            messages_to_prompt(messages),
        )

    def test_trims_history_and_preserves_project_brief_and_latest_request(self):
        messages = [
            {"role": "user", "content": "Build a complete inventory app with authentication."},
            {"role": "assistant", "content": "huge previous reply " * 100},
            {"role": "user", "content": "Also include deployment instructions."},
            {"role": "assistant", "content": "short implementation note"},
            {"role": "user", "content": "Finish the app and include all files."},
        ]

        prompt = messages_to_prompt(messages, max_bytes=600)

        self.assertLessEqual(len(prompt.encode("utf-8")), 600)
        self.assertIn("Build a complete inventory app", prompt)
        self.assertIn("Finish the app and include all files.", prompt)
        self.assertIn("deployment instructions", prompt)
        self.assertIn("short implementation note", prompt)
        self.assertIn("history was omitted", prompt)
        self.assertNotIn("huge previous reply", prompt)

    def test_skips_oversized_recent_message_and_keeps_other_fitting_context(self):
        messages = [
            {"role": "user", "content": "Original app requirements."},
            {"role": "assistant", "content": "Useful earlier response."},
            {"role": "assistant", "content": "very large " * 100},
            {"role": "user", "content": "Continue the original app."},
        ]

        prompt = messages_to_prompt(messages, max_bytes=500)

        self.assertIn("Original app requirements.", prompt)
        self.assertIn("Useful earlier response.", prompt)
        self.assertIn("Continue the original app.", prompt)
        self.assertNotIn("very large", prompt)

    def test_rejects_latest_message_that_exceeds_limit(self):
        with self.assertRaisesRegex(PromptTooLong, "latest message and system instructions"):
            messages_to_prompt([{"role": "user", "content": "x" * 101}], max_bytes=100)

    def test_counts_json_escaped_unicode_bytes(self):
        with self.assertRaises(PromptTooLong):
            messages_to_prompt([{"role": "user", "content": "🙂" * 20}], max_bytes=100)

    def test_keeps_system_instruction_when_trimming_history(self):
        messages = [
            {"role": "system", "content": "Keep answers concise."},
            {"role": "user", "content": "old " * 40},
            {"role": "user", "content": "latest question"},
        ]

        prompt = messages_to_prompt(messages, max_bytes=600)

        self.assertIn("Keep answers concise.", prompt)
        self.assertIn("latest question", prompt)


class CompactionRetryTests(unittest.TestCase):
    def test_retries_413_with_smaller_history_and_keeps_project_requirements(self):
        class Client:
            def __init__(self):
                self.prompts = []

            def start_conversation(self, prompt, model, history_disabled):
                self.prompts.append(prompt)
                if len(prompt.encode("utf-8")) > 30000:
                    raise ChatGPTError("request too large", status=413)
                return "response"

        client = Client()
        messages = [
            {"role": "user", "content": "Initial project requirements. " * 300},
            {"role": "assistant", "content": "large previous response. " * 1400},
            {"role": "user", "content": "Latest project changes."},
        ]
        retry_logs = []

        prompt, response, compacted = start_conversation_with_compaction(
            client, messages, "gpt-6", history_disabled=True, on_retry=retry_logs.append)

        self.assertEqual(response, "response")
        self.assertTrue(compacted)
        self.assertGreater(len(client.prompts), 1)
        self.assertIn("Initial project requirements.", prompt)
        self.assertIn("Latest project changes.", prompt)
        self.assertNotIn("large previous response.", prompt)
        self.assertEqual(len(retry_logs), len(client.prompts) - 1)

    def test_does_not_retry_non_size_upstream_error(self):
        class Client:
            def __init__(self):
                self.calls = 0

            def start_conversation(self, prompt, model, history_disabled):
                self.calls += 1
                raise ChatGPTError("unauthorized", status=401)

        client = Client()
        with self.assertRaises(ChatGPTError) as error:
            start_conversation_with_compaction(
                client, [{"role": "user", "content": "Hello"}], "gpt-6", True)

        self.assertEqual(error.exception.status, 401)
        self.assertEqual(client.calls, 1)


if __name__ == "__main__":
    unittest.main()
