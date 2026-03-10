"""Manual annotations for the spaCy NER benchmark sample."""

from __future__ import annotations

from typing import Any


def E(text: str, label: str) -> dict[str, str]:
    return {"text": text, "label": label}


def item(
    title_countries: list[str],
    title_entities: list[dict[str, str]],
    *,
    title_description_countries: list[str] | None = None,
    title_description_entities: list[dict[str, str]] | None = None,
    notes: list[str] | None = None,
    score_title_countries: bool = True,
    score_title_entities: bool = True,
    score_title_description_countries: bool = True,
    score_title_description_entities: bool = True,
) -> dict[str, Any]:
    return {
        "title": {
            "countries": title_countries,
            "entities": title_entities,
            "score_countries": score_title_countries,
            "score_entities": score_title_entities,
        },
        "title_description": {
            "countries": (
                title_description_countries
                if title_description_countries is not None
                else title_countries
            ),
            "entities": (
                title_description_entities
                if title_description_entities is not None
                else title_entities
            ),
            "score_countries": score_title_description_countries,
            "score_entities": score_title_description_entities,
        },
        "notes": notes or [],
    }


GOLD_ANNOTATIONS: dict[str, dict[str, Any]] = {
    # obvious_country_title
    "e1726382f53e7b4c": item(
        ["China"],
        [E("Sinopec", "ORG"), E("China", "GPE")],
    ),
    "af167c56545f6ae0": item(
        ["Zimbabwe"],
        [E("Zimbabwe", "GPE"), E("Ministry of Mines and Mining Development", "ORG")],
        notes=["Description-only ministry mention kept because it is central to the ban."],
    ),
    "93f7346e3b3dc15b": item(
        ["Argentina"],
        [E("Argentina", "GPE")],
    ),
    "d45ef8f9a7eafb1a": item(
        ["United States", "Venezuela"],
        [E("US", "GPE"), E("Trump", "PERSON"), E("Venezuela", "GPE")],
    ),
    "9fd228a2e5537566": item(
        ["China"],
        [E("China", "GPE")],
    ),
    "b940c10d3334b185": item(
        ["India", "Iran"],
        [E("Indian", "NORP"), E("Iran", "GPE")],
    ),
    "51b2e3f29825008f": item(
        ["China"],
        [E("China", "GPE"), E("Shanghai SECCO", "ORG")],
    ),
    "194f383785e2efb1": item(
        ["United States"],
        [E("Dow", "ORG"), E("US", "GPE")],
    ),
    "1b729ac5f0953020": item(
        ["Oman"],
        [E("Oman", "GPE")],
    ),
    "d541d8e3f259e12b": item(
        ["United States", "Iran"],
        [E("US", "GPE"), E("Iran", "GPE"), E("Europe", "LOC")],
    ),
    "a50f0a4055cd12fa": item(
        ["United States"],
        [E("US E15 group", "ORG")],
    ),
    "c8fa2546d3e9b9ea": item(
        ["Argentina"],
        [E("Argentina", "GPE")],
    ),
    "9f34fd5bb7ae9d73": item(
        ["United Arab Emirates"],
        [E("UAE", "GPE"), E("Fujairah", "GPE")],
    ),
    "c42e259a700ef165": item(
        ["Malaysia"],
        [E("Malaysia", "GPE"), E("Fathopes", "ORG")],
    ),
    "02aadbeb2a81191a": item(
        ["United States"],
        [E("US", "GPE"), E("Devon", "ORG"), E("Coterra", "ORG")],
    ),
    "24c25b15c07902ee": item(
        ["Japan"],
        [E("Japan", "GPE")],
    ),

    # abbreviation_title
    "b00a49b16ff04886": item(
        ["United States"],
        [E("US", "GPE")],
    ),
    "d91ab8516896dda7": item(
        ["United States", "Venezuela"],
        [E("US", "GPE"), E("Venezuela", "GPE")],
    ),
    "e9c32a2cf33b0047": item(
        ["United States", "Venezuela"],
        [E("US", "GPE"), E("Maduro", "PERSON"), E("Venezuela", "GPE")],
    ),
    "131c5a0d28185d11": item(
        ["Venezuela", "United States"],
        [E("Venezuela", "GPE"), E("US", "GPE")],
    ),
    "836dbffe91157c3b": item(
        ["United States", "Bahrain"],
        [E("US", "GPE"), E("Bahrain", "GPE")],
    ),
    "327532c1edc3ed59": item(
        ["United States"],
        [E("NYMEX", "ORG"), E("US", "GPE")],
    ),
    "3ff847ce9ffa22e7": item(
        ["United States"],
        [E("US", "GPE"), E("Mideast Gulf", "LOC")],
    ),
    "cfaaaf85082bb2eb": item(
        ["United States"],
        [E("Valero", "ORG"), E("Marathon", "ORG"), E("US", "GPE")],
    ),
    "0d32015cb82da3fd": item(
        ["United States", "Iran"],
        [E("Asia", "LOC"), E("US", "GPE"), E("Iran", "GPE")],
        title_description_entities=[
            E("Asia", "LOC"),
            E("US", "GPE"),
            E("Iran", "GPE"),
            E("Middle East", "LOC"),
        ],
    ),
    "8f66e9903ca8f1a3": item(
        ["United States"],
        [E("US", "GPE"), E("Tesla", "ORG")],
    ),
    "717904c7af98ded0": item(
        ["United States"],
        [E("US", "GPE"), E("DOE", "ORG")],
    ),
    "58413418401b5eff": item(
        ["United States"],
        [E("US", "GPE")],
    ),

    # company_heavy
    "e9735955648fbf35": item(
        ["Germany"],
        [E("BASF", "ORG"), E("Ludwigshafen", "GPE"), E("Germany", "GPE")],
    ),
    "603c20d792e11805": item(
        [],
        [E("ADNOC", "ORG")],
        title_description_countries=["United Arab Emirates"],
        title_description_entities=[
            E("ADNOC", "ORG"),
            E("Ruwais", "GPE"),
            E("United Arab Emirates", "GPE"),
        ],
    ),
    "ae38e3dc2d825848": item(
        ["Saudi Arabia"],
        [E("Aramco", "ORG"), E("Ras Tanura", "GPE")],
    ),
    "ad15b4f74c7f7bd4": item(
        ["China"],
        [E("Sinopec", "ORG"), E("China", "GPE")],
    ),
    "af9c34955bf0314a": item(
        [],
        [E("CBAM", "LAW"), E("Yara", "ORG")],
        notes=["CBAM treated as a salient policy entity, not a country."],
    ),
    "5ad34d064c35f1f7": item(
        ["United Kingdom"],
        [E("Storengy UK", "ORG"), E("UK", "GPE"), E("British", "NORP")],
    ),
    "a079cb6af04e843f": item(
        ["Brazil"],
        [E("Brazil", "GPE"), E("Bahiagas", "ORG"), E("Petrobras", "ORG")],
    ),
    "2df4df42ac741f99": item(
        ["United Kingdom"],
        [E("UK", "GPE"), E("Tata", "ORG")],
    ),
    "adea7de280bf639e": item(
        [],
        [E("Aramco", "ORG"), E("Hormuz", "LOC")],
    ),
    "8e6c3d52b0b24b62": item(
        [],
        [E("Aramco", "ORG")],
    ),
    "291d9a8863853879": item(
        ["China"],
        [E("China", "GPE"), E("Sinopec", "ORG")],
    ),
    "bb0f11d9f5750abe": item(
        ["Japan"],
        [E("Japan", "GPE"), E("ENEOS Corp", "ORG"), E("Mideast", "LOC")],
    ),

    # geopolitical
    "12cafb5ef3c1076f": item(
        ["Venezuela"],
        [E("Opec+", "ORG"), E("Venezuela", "GPE")],
    ),
    "1e2267fcd7d4ce84": item(
        [],
        [E("Europe", "LOC"), E("Middle East", "LOC")],
        title_description_countries=["United States", "Iran"],
        title_description_entities=[
            E("Europe", "LOC"),
            E("Middle East", "LOC"),
            E("US", "GPE"),
            E("Iran", "GPE"),
        ],
    ),
    "81200c7ffa2e300a": item(
        ["Vietnam", "United States", "Iran"],
        [E("Vietnam", "GPE"), E("US", "GPE"), E("Iran", "GPE")],
    ),
    "4c896ead51d91c5e": item(
        ["United States", "Belarus"],
        [E("US", "GPE"), E("Belarus", "GPE")],
    ),
    "228abeec7a355d5e": item(
        [],
        [E("Middle East", "LOC"), E("Methanex", "ORG")],
        title_description_countries=["United States", "Iran"],
        title_description_entities=[
            E("Middle East", "LOC"),
            E("Methanex", "ORG"),
            E("US", "GPE"),
            E("Iran", "GPE"),
            E("Rich Sumner", "PERSON"),
        ],
    ),
    "fcfb106bfde3a925": item(
        [],
        [E("Middle East", "LOC")],
    ),
    "0a3becb254330ee9": item(
        ["United Arab Emirates"],
        [E("Fujairah", "GPE"), E("UKMTO", "ORG")],
    ),
    "51adf164dc538e3a": item(
        [],
        [E("Hormuz", "LOC"), E("Opec+", "ORG")],
    ),
    "4b6910d4a23a8075": item(
        [],
        [E("Asia", "LOC"), E("Middle East", "LOC")],
        title_description_countries=["China"],
        title_description_entities=[
            E("Asia", "LOC"),
            E("Middle East", "LOC"),
            E("China", "GPE"),
        ],
    ),
    "9f7b6ef37033124b": item(
        ["United States", "Iran"],
        [E("US", "GPE"), E("Iran", "GPE")],
    ),
    "a17ef1def566db00": item(
        [],
        [E("Middle East", "LOC")],
    ),
    "a81d937f87f74394": item(
        ["Iran", "India"],
        [E("Iran", "GPE"), E("India", "GPE")],
    ),

    # commodity_weak_geo
    "ed777cfab6d35905": item(
        ["Guyana"],
        [E("Guyanese", "NORP")],
    ),
    "1e3a57b22de9e6eb": item(
        [],
        [E("TTF", "ORG")],
        notes=["TTF is treated as a salient benchmark/hub entity despite terse market-summary wording."],
    ),
    "7613ace8ff7d6b9f": item(
        ["South Korea"],
        [E("S Korea", "GPE")],
    ),
    "733e136b8043e682": item(
        [],
        [E("IEA", "ORG")],
        title_description_countries=["China"],
        title_description_entities=[
            E("IEA", "ORG"),
            E("China", "GPE"),
            E("OPEC+", "ORG"),
        ],
    ),
    "d3ae2be1c174db2b": item(
        ["Indonesia"],
        [E("Indonesian", "NORP")],
    ),
    "6e192f69a56e2fd4": item(
        ["France"],
        [E("French", "NORP")],
    ),
    "c46ea90dbc833438": item(
        ["Angola"],
        [E("Cabinda", "GPE")],
    ),
    "d5c0f15c92f00918": item(
        ["Venezuela"],
        [E("Venezuelan", "NORP"), E("Mediterranean", "LOC")],
    ),
    "c0517cca48f16dd9": item(
        ["United States"],
        [E("Alaska", "GPE"), E("North Slope", "LOC")],
    ),
    "245774e6ccbf4231": item(
        ["Germany"],
        [E("German", "NORP")],
    ),
    "493bc6440b3fcbc6": item(
        [],
        [E("Lighthizer", "PERSON")],
    ),
    "50f83d04198af61f": item(
        ["United States"],
        [E("Houston", "GPE")],
    ),
    "20fbca12e1c19116": item(
        ["United States"],
        [E("Lake Charles LNG", "FAC")],
    ),
    "dc4f0613e161e95b": item(
        [],
        [E("west African", "NORP")],
    ),
    "5c3ef9d02c12d279": item(
        [],
        [E("TTF", "ORG")],
        notes=["TTF is treated as a salient benchmark/hub entity."],
    ),
    "439701fe348227e7": item(
        ["United States"],
        [E("Woodside", "ORG"), E("Texas", "GPE")],
    ),

    # description_geo_only_or_disagree
    "038fb54667409fd5": item(
        ["United States"],
        [E("Trinseo", "ORG"), E("New York Stock Exchange", "ORG")],
        notes=["New York in the title is treated as an unambiguous US location; London dateline is ignored."],
    ),
    "fac8b8bd8d884b9c": item(
        [],
        [E("Middle East", "LOC")],
        title_description_countries=["China"],
        title_description_entities=[
            E("Middle East", "LOC"),
            E("China", "GPE"),
            E("Gulf Cooperation Council", "ORG"),
        ],
    ),
    "6e4887b23c2bf00c": item(
        [],
        [E("Europe", "LOC")],
    ),
    "508c0228c336d855": item(
        ["South Korea"],
        [E("S Korea", "GPE"), E("S-Oil", "ORG")],
        title_description_entities=[
            E("S Korea", "GPE"),
            E("S-Oil", "ORG"),
            E("Onsan", "GPE"),
        ],
    ),
    "8867ebc5b68f8912": item(
        ["Australia"],
        [E("Australia", "GPE"), E("Mayfair", "ORG"), E("Phosphate Hill", "FAC")],
        title_description_entities=[
            E("Australia", "GPE"),
            E("Mayfair", "ORG"),
            E("Phosphate Hill", "FAC"),
            E("Dyno Nobel", "ORG"),
            E("Queensland", "GPE"),
        ],
    ),
    "efe404418ee7733b": item(
        ["United States", "Iran"],
        [E("US", "GPE"), E("Iran", "GPE")],
        title_description_entities=[
            E("US", "GPE"),
            E("Iran", "GPE"),
            E("Strait of Hormuz", "LOC"),
        ],
        notes=["Dateline London is ignored; the fuzzy Virgin Islands match should score as a false positive."],
    ),
    "e83ccbe758a79098": item(
        ["China"],
        [E("East China", "LOC")],
    ),
    "2b41bc23e9e91cd3": item(
        [],
        [E("Middle East", "LOC")],
        title_description_countries=["United States", "Iran"],
        title_description_entities=[
            E("Middle East", "LOC"),
            E("US", "GPE"),
            E("Iran", "GPE"),
        ],
    ),
    "ebb3dc5e3a22e4d0": item(
        ["Portugal", "Spain"],
        [E("Portugal", "GPE"), E("Spain", "GPE")],
    ),
    "b3064bf14e390491": item(
        [],
        [E("Asia", "LOC")],
    ),
    "d38a9b47c30efbc3": item(
        ["Thailand"],
        [E("Thailand", "GPE"), E("PTTGC", "ORG")],
    ),
    "1a4b3b39febdd0cc": item(
        [],
        [E("Mideast", "LOC")],
        title_description_countries=["Qatar", "Bahrain", "United Arab Emirates"],
        title_description_entities=[
            E("Mideast", "LOC"),
            E("Qatar", "GPE"),
            E("Bahrain", "GPE"),
            E("UAE", "GPE"),
        ],
    ),
    "3423d6902eb6fd89": item(
        ["China"],
        [E("China", "GPE"), E("ZPC", "ORG")],
    ),
    "568adc222a823918": item(
        ["China"],
        [E("China", "GPE"), E("Sinopec", "ORG")],
    ),
    "cb62c3062c2d3938": item(
        [],
        [E("Europe", "LOC")],
        title_description_countries=["United States", "Iran"],
        title_description_entities=[
            E("Europe", "LOC"),
            E("US", "GPE"),
            E("Iran", "GPE"),
        ],
    ),
    "48aadbc5c29cf83c": item(
        ["United States", "Iran"],
        [E("US", "GPE"), E("Iran", "GPE")],
    ),
}
