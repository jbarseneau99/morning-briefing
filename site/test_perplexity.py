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
BLOG = 'https://www.example.com/blog/example-story-d'
D = 'https://apnews.com/article/example-story-e'


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


class PickPublisherUrlTest(unittest.TestCase):
    def test_best_ranked_source_wins(self):
        # example.com is unweighted (5); Reuters and BBC are 10; NYT is 9.
        self.assertEqual(edition.pick_publisher_url([BLOG, A, B, C]), A)

    def test_tied_top_weight_keeps_first_seen(self):
        # Reuters and BBC both 10; first-seen Reuters wins, not later BBC.
        self.assertEqual(edition.pick_publisher_url([A, B, C]), A)
        self.assertEqual(edition.pick_publisher_url([B, A, C]), B)

    def test_first_verified_preferred_over_higher_weight(self):
        # Verified blog URL is kept even though Reuters ranks higher.
        self.assertEqual(
            edition.pick_publisher_url([BLOG, A, B], verified_urls=[BLOG]),
            BLOG,
        )

    def test_best_ranked_among_verified(self):
        # NYT (9) and Reuters (10) are both verified; Reuters wins.
        self.assertEqual(
            edition.pick_publisher_url([BLOG, C, A], verified_urls=[C, A]),
            A,
        )

    def test_empty(self):
        self.assertIsNone(edition.pick_publisher_url([]))


class PerplexityCandidatesTest(unittest.TestCase):
    def test_one_candidate_per_row_picks_best_ranked(self):
        rows = [{'headline': 'Senate bill', 'summary': 'Passed.',
                 'publisher_urls': [BLOG, A, B, C]}]
        pplx_urls, cands = edition.perplexity_candidates(rows)
        self.assertEqual([c['url'] for c in cands], [A])
        self.assertEqual(len(cands), 1)
        self.assertTrue(cands[0]['from_perplexity'] and cands[0]['feed'] == 'perplexity')
        self.assertEqual(cands[0]['title'], 'Senate bill')
        self.assertEqual(cands[0]['desc'], 'Passed.')
        self.assertEqual(cands[0]['pplx_urls'], [BLOG, A, B, C])
        self.assertEqual(
            pplx_urls,
            {edition.canon(BLOG), edition.canon(A), edition.canon(B), edition.canon(C)},
        )

    def test_first_verified_url_is_emitted(self):
        rows = [{'headline': 'Verified first', 'publisher_urls': [BLOG, A, B],
                 'verified_urls': [BLOG]}]
        _, cands = edition.perplexity_candidates(rows)
        self.assertEqual([c['url'] for c in cands], [BLOG])

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

    def test_row_already_in_edition_drops_all_siblings(self):
        rows = [
            {'headline': 'Already in', 'summary': 'One story.',
             'publisher_urls': [A, B, C]},
            {'headline': 'Fresh', 'url': D},
        ]
        pplx_urls, cands = edition.perplexity_candidates(
            rows, used_urls={edition.canon(B)},
        )
        # Sibling B is in the edition, so A and C are also dropped.
        self.assertEqual([c['url'] for c in cands], [D])
        self.assertEqual(cands[0]['title'], 'Fresh')
        # Scoring set still includes every URL, including the dropped row.
        self.assertEqual(
            pplx_urls,
            {edition.canon(A), edition.canon(B), edition.canon(C), edition.canon(D)},
        )

    def test_used_url_matches_canonical_form(self):
        rows = [{'headline': 'Canon match', 'publisher_urls': [A, B, C]}]
        _, cands = edition.perplexity_candidates(rows, used_urls={edition.canon(A_DUP)})
        self.assertEqual(cands, [])

    def test_prior_window_url_drops_row(self):
        # Last-3-day window is passed in as used_urls the same way as today.
        rows = [{'headline': 'From yesterday', 'publisher_urls': [BLOG, C]}]
        _, cands = edition.perplexity_candidates(rows, used_urls={edition.canon(C)})
        self.assertEqual(cands, [])

    def test_no_used_urls_still_one_per_row(self):
        rows = [
            {'headline': 'One', 'publisher_urls': [A, B, C]},
            {'headline': 'Two', 'publisher_urls': [BLOG, D]},
        ]
        _, cands = edition.perplexity_candidates(rows)
        self.assertEqual([c['title'] for c in cands], ['One', 'Two'])
        self.assertEqual([c['url'] for c in cands], [A, D])


if __name__ == '__main__':
    unittest.main()
