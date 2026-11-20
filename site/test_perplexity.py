#!/usr/bin/env python3
"""Unit tests for Perplexity snapshot URL shapes (publisher_url / url / publisher_urls)."""
from __future__ import annotations

import unittest

import edition


A = 'https://www.reuters.com/world/example-story-a/'
B = 'https://www.bbc.com/news/example-story-b'
C = 'http://www.nytimes.com/2026/11/20/world/example-story-c.html?utm_source=rss'
# Same story as A after canon (www/scheme/trailing slash).
A_DUP = 'http://reuters.com/world/example-story-a'


class PublisherLinksShapesTest(unittest.TestCase):
    def test_publisher_url_string(self):
        urls = edition.publisher_links({'publisher_url': A, 'headline': 'A'})
        self.assertEqual(urls, [A])

    def test_url_string(self):
        urls = edition.publisher_links({'url': B, 'title': 'B'})
        self.assertEqual(urls, [B])

    def test_publisher_urls_list(self):
        urls = edition.publisher_links({'publisher_urls': [A, B, C], 'headline': 'List'})
        self.assertEqual(urls, [A, B, C])

    def test_publisher_urls_string(self):
        urls = edition.publisher_links({'publisher_urls': A})
        self.assertEqual(urls, [A])

    def test_publisher_url_list(self):
        urls = edition.publisher_links({'publisher_url': [A, B]})
        self.assertEqual(urls, [A, B])

    def test_url_list(self):
        urls = edition.publisher_links({'url': [B, C]})
        self.assertEqual(urls, [B, C])

    def test_mixed_keys_first_seen_order_and_canon_dedup(self):
        urls = edition.publisher_links({
            'publisher_url': A,
            'url': B,
            'publisher_urls': [A_DUP, C, B],
        })
        self.assertEqual(urls, [A, B, C])

    def test_publisher_url_preferred_over_url_when_both_strings(self):
        urls = edition.publisher_links({'publisher_url': A, 'url': B})
        self.assertEqual(urls, [A, B])
        # Existing single-field preference: publisher_url is first / primary.
        self.assertEqual(urls[0], A)

    def test_invalid_and_empty_skipped(self):
        urls = edition.publisher_links({
            'publisher_url': '  ',
            'url': 'ftp://example.com/not-http',
            'publisher_urls': [None, 123, '', 'not-a-url', '  ' + A + '  ', A],
        })
        self.assertEqual(urls, [A])

    def test_non_dict_is_empty(self):
        self.assertEqual(edition.publisher_links(None), [])
        self.assertEqual(edition.publisher_links('https://example.com'), [])
        self.assertEqual(edition.publisher_links([]), [])


class PerplexityCandidatesTest(unittest.TestCase):
    def test_each_list_url_is_a_candidate(self):
        rows = [{'headline': 'Senate bill', 'summary': 'Passed.', 'publisher_urls': [A, B, C]}]
        pplx_urls, cands = edition.perplexity_candidates(rows)
        self.assertEqual([c['url'] for c in cands], [A, B, C])
        self.assertEqual(len(cands), 3)
        self.assertTrue(all(c['from_perplexity'] and c['feed'] == 'perplexity' for c in cands))
        self.assertEqual(cands[0]['title'], 'Senate bill')
        self.assertEqual(cands[0]['desc'], 'Passed.')
        self.assertEqual(pplx_urls, {edition.canon(A), edition.canon(B), edition.canon(C)})

    def test_publisher_url_string_is_one_candidate(self):
        rows = [{'headline': 'One', 'publisher_url': A}]
        pplx_urls, cands = edition.perplexity_candidates(rows)
        self.assertEqual([c['url'] for c in cands], [A])
        self.assertIn(edition.canon(A), pplx_urls)

    def test_url_string_is_one_candidate(self):
        rows = [{'title': 'Via url', 'notes': 'Note.', 'url': B, 'publisher': 'BBC'}]
        pplx_urls, cands = edition.perplexity_candidates(rows)
        self.assertEqual([c['url'] for c in cands], [B])
        self.assertEqual(cands[0]['title'], 'Via url')
        self.assertEqual(cands[0]['desc'], 'Note.')
        self.assertEqual(cands[0]['src'], 'BBC')
        self.assertIn(edition.canon(B), pplx_urls)

    def test_unverified_contributes_to_url_set_but_not_candidates(self):
        rows = [
            {'headline': 'Skip me', 'publisher_url': A, 'verified': False},
            {'headline': 'Keep me', 'url': B, 'verified': True},
        ]
        pplx_urls, cands = edition.perplexity_candidates(rows)
        self.assertEqual([c['url'] for c in cands], [B])
        self.assertEqual(pplx_urls, {edition.canon(A), edition.canon(B)})

    def test_rows_without_links_are_ignored(self):
        rows = [
            {'headline': 'No links', 'publisher_urls': []},
            {'headline': 'Also none', 'url': ''},
            {},
        ]
        pplx_urls, cands = edition.perplexity_candidates(rows)
        self.assertEqual(cands, [])
        self.assertEqual(pplx_urls, set())


if __name__ == '__main__':
    unittest.main()
