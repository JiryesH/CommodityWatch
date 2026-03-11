"""Manual country annotations for the unseen holdout NER audit."""

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


HOLDOUT_COUNTRY_ANNOTATIONS: dict[str, dict[str, Any]] = {
    "5cdd4dfaac573867": item(["Qatar"]),
    "523128ee53e571bc": item(["Australia"]),
    "412495f395ee8390": item(
        ["China", "Brazil"],
        title_description_countries=["China", "Brazil", "United Kingdom"],
    ),
    "16de260d582fef9a": item(["United States", "Iran"]),
    "a46fe5871d34bd03": item(["United States"]),
    "45ad5a0ba25ffab5": item(
        ["United Arab Emirates"],
        notes=["Abu Dhabi counted as an unambiguous UAE location."],
    ),
    "0e75a759ff370c66": item(["Morocco"]),
    "5a41b0e37c52220d": item(["China"]),
    "5ae43ebc41249b0b": item(["China"]),
    "f5a13bafb3204bda": item(["United States", "South Korea"]),
    "d2fbc75547d1bea3": item(["Australia"]),
    "a24fb3f0d86f1cca": item(["United States"]),
    "028e69302d6a79b0": item(["United States"]),
    "e005b42fc33e94cf": item(["United States"]),
    "5f085d104a24d460": item(["United States"]),
    "a09bb0d075a16010": item(["United States"]),
    "acac10fa231da784": item(["United States"]),
    "a4ce77d94be46e8d": item(["United States", "Venezuela"]),
    "992e2d7af0af75d7": item(["United States", "Bangladesh"]),
    "b8812d6830248295": item(["Japan", "United States"]),
    "6d6b8abda4e957ff": item([]),
    "d9f27ff3cc256ce8": item(["Nigeria"]),
    "3b00b12dcff5009b": item([]),
    "f6aeb08eda178059": item(["United Kingdom"]),
    "759c814d2393d5b6": item(["United Arab Emirates"]),
    "a7967da13c4e9a00": item(["Poland"]),
    "528c1283d8bef23a": item(["Netherlands", "Belgium"]),
    "6a1b7a082eb3780e": item(
        [],
        title_description_countries=["Poland"],
        notes=["Plock left out of title-only gold as indirect; description makes Poland explicit."],
    ),
    "b54b4c17edc245e4": item(["India", "Switzerland"]),
    "3a3d0d0b764f1de9": item(["South Korea"]),
    "14c00dfa33a31ee6": item(["United States", "Iran"]),
    "ec7047873ff54aa9": item(["Cuba"]),
    "51add781d0754360": item(["United States"]),
    "859a1d885aa4c399": item([]),
    "2224c5eebece53bf": item(["Iran"]),
    "9cbff5d53313e692": item(["United States", "India", "Iran"]),
    "8593e87fafaf5cd5": item(["China", "United States", "Iran"]),
    "55c290ea63c5e4bc": item(
        [],
        title_description_countries=["United States", "Iran"],
    ),
    "680c2afb750cddc9": item(
        ["United States"],
        notes=["Sabine Pass counted as a clear US facility/location."],
    ),
    "d0b7e6e0840d3eaa": item(["South Korea"]),
    "d63e67479a8d40ae": item(["Saudi Arabia"]),
    "c0ebfdf65896bddd": item(["Australia"]),
    "5d7cf2fe040c04a5": item(
        [],
        title_description_countries=["France"],
    ),
    "c00eb4a956cade49": item(
        ["Spain"],
        notes=["Bilbao counted as a clear Spain location."],
    ),
    "ed997d191efc5ed3": item(
        [],
        title_description_countries=["Switzerland"],
    ),
    "7490a0caefba3187": item(["Indonesia"]),
    "72b348def32d7b38": item([]),
    "f2a56c1b4d6a9aa9": item(["Thailand"]),
    "347dcaa4bfd8cd16": item(["Germany"]),
    "8ede72bfea870e69": item([]),
    "019efd6b1cbdc8da": item(
        [],
        title_description_countries=["Iran"],
    ),
    "3976d7b1b7639066": item(
        [],
        title_description_countries=["United States"],
    ),
    "65ad385a52337338": item([]),
    "c87434c2c9aa2f68": item(
        ["China"],
        title_description_countries=["China", "United States", "Iran", "Israel"],
        notes=["Israeli counted as evidence for Israel in description mode."],
    ),
    "a96c8f40664574db": item(["Taiwan"]),
    "c7043d1222be724c": item([]),
    "7e00a6729b10d9ed": item(["United States", "Iran"]),
    "7ed04c5f08336d52": item(
        [],
        title_description_countries=["Japan"],
    ),
    "d694d123fff56e35": item([]),
    "40781579eff9b06f": item(
        [],
        notes=["Singapore time reference treated as non-geographic metadata, not article country context."],
    ),
}
