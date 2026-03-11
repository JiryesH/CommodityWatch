"""Manual country annotations for the second unseen holdout NER audit."""

from __future__ import annotations

from typing import Any


def item(
    title_countries: list[str],
    *,
    title_description_countries: list[str] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "title": {
            "countries": title_countries,
            "entities": [],
            "score_countries": True,
            "score_entities": False,
        },
        "title_description": {
            "countries": (
                title_description_countries
                if title_description_countries is not None
                else title_countries
            ),
            "entities": [],
            "score_countries": True,
            "score_entities": False,
        },
        "notes": notes or [],
    }


HOLDOUT2_COUNTRY_ANNOTATIONS: dict[str, dict[str, Any]] = {
    "80d3dabee9dd212e": item(["Mexico"]),
    "31ac35b1ec13f3eb": item(["India", "United States"]),
    "f4286c1c5ad1cb24": item(["Australia"]),
    "a2f96bd9f89f21e4": item(
        ["United States", "Venezuela"],
        notes=["Trinidad left out as indirect shorthand for Trinidad and Tobago in title-only mode."],
    ),
    "ce18d20f764f0de5": item(["Venezuela"]),
    "fee8e3620597f426": item(["India"]),
    "6d34e91039a67ed2": item(["United States"]),
    "c2fcfbe71ecf99f8": item(["Brazil"]),
    "4a25f70353f9f607": item(
        ["United Arab Emirates"],
        notes=["Dubai counted as an unambiguous UAE location."],
    ),
    "be20291b990f574f": item(["United States", "Venezuela"]),
    "0ac09c234eab5f29": item(["Mexico"]),
    "ee3ac1ff5bc37dec": item(["Australia", "United States"]),
    "be203ab656250e19": item(["Iran", "United States"]),
    "1d9e66c11e5e131e": item(
        ["United Arab Emirates"],
        title_description_countries=["Iran", "United Arab Emirates", "United States"],
        notes=["Singapore dateline ignored in description mode."],
    ),
    "3940877b45e17949": item(["United States"]),
    "4ef22ab7faa0ef2f": item(["Iran", "United States"]),
    "307fa0506fdeab1f": item(["Iran", "United States"]),
    "8528c0cc68d2088f": item(["Oman", "United Arab Emirates"]),
    "016aa73f2ded4b71": item(["United States"]),
    "2479c67d082ad3cf": item(
        ["Japan", "United States"],
        notes=["Japan counted despite mojibake in the stored title text."],
    ),
    "dc495a6270c0fbd0": item(
        ["Saudi Arabia"],
        notes=["Saudi counted as a standalone demonym in the company name."],
    ),
    "77816d8f4ab77864": item([]),
    "941fa3be71cc92e7": item(
        ["United Arab Emirates"],
        notes=["Abu Dhabi counted as an unambiguous UAE location."],
    ),
    "28f9e5699b56c8af": item(["United Kingdom"]),
    "3c66254cc1d5a3a5": item(["China"]),
    "2a06f5c8c424fc87": item(["China"]),
    "7d7b831058ded6e8": item(["United Kingdom"]),
    "9de0db8586774e52": item(
        ["United Arab Emirates"],
        notes=["Abu Dhabi counted as an unambiguous UAE location."],
    ),
    "67f70aa51dab02fb": item(["India"]),
    "1b563895bdcc9ae5": item(["Iran", "United States"]),
    "4a09c3407fc08c95": item(["Iran", "Kuwait", "United States"]),
    "9420bc2632ce1695": item([]),
    "48397ea0073773d2": item(["Iran", "United States"]),
    "50941e0f0cdd888e": item(["Thailand"]),
    "53f82f90cb6c31a0": item([]),
    "34dd8f0e12f48181": item(
        [],
        title_description_countries=["Philippines"],
        notes=["Philippine Department of Agriculture counted as evidence for the Philippines in description mode."],
    ),
    "118530e9009dae46": item(["Iran", "United States"]),
    "3c9a491950c011c4": item(["Greenland"]),
    "351a577b996888f6": item([]),
    "f8403825e9305fb8": item([]),
    "95fa9f20e29f4564": item(
        [],
        notes=["QatarEnergy treated as a fused company brand, not a standalone country mention."],
    ),
    "9542c427a4d7e5d0": item(["South Korea"]),
    "9e2dbaa5cf4a0d81": item(
        [],
        title_description_countries=["China"],
        notes=["Chinese counted as evidence for China in description mode."],
    ),
    "1761067245be631f": item(["Romania"]),
    "c24aa462d56874b0": item([]),
    "4bf47f3387fe4d1c": item(["India"]),
    "fea26112ffaf6d22": item(["United States"]),
    "69daf763b5e9eaca": item(["Kazakhstan"]),
    "b1fa7c44a81c72bd": item([]),
    "1798af8d393a1bed": item(
        [],
        notes=["VIP Iberico left out because the available title/description snippet does not make a country explicit."],
    ),
    "45cfbf0e0dbb8d56": item(["China"]),
    "aa3ac2824a92f506": item([]),
    "12377e181b58f54b": item(
        [],
        title_description_countries=["Iran", "United States"],
    ),
    "62a6ff029ad5e2eb": item(["Japan"]),
    "77199ab75451e2ce": item(
        [],
        title_description_countries=["China"],
    ),
    "bb75c843192ae466": item(["China"]),
    "23ecd43aaff4a1de": item(["Australia"]),
    "c5d3c92e847f0b7f": item(
        [],
        title_description_countries=["United States"],
    ),
    "75ddfeb479310a6b": item(["Australia"]),
    "966f72377129364b": item([]),
}
