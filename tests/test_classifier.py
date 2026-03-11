from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from classifier import (
    classify_categories,
    classify_category,
    explain_classification,
    normalize_article_categories,
)


@pytest.mark.parametrize(
    ("title", "description", "expected"),
    [
        (
            "Italian gas-grid operator unfazed by seasonal backwardation ahead of injection season",
            "Snam CEO says no volume issues for Italy at present despite backwardation "
            "Energy decree described as 'positive move' Still time to support gas "
            "injection, CEO maintains MILAN (ICIS)--Italian gas...",
            ["Natural Gas"],
        ),
        (
            "INSIGHT: Middle East conflict drives global chemical spot prices to one of biggest spikes this decade",
            "LONDON (ICIS)--Following the outbreak of the US-Iran conflict, key chemical "
            "spot prices across regions surged with the week-on-week increase comparable "
            "to the biggest spikes in any week over the past...",
            ["Chemicals"],
        ),
        (
            "Plant status: Saudi Arabia's Sadara declares FM on glycol ethers at Al Jubail",
            "LONDON (ICIS)--Here is a plant status report: Name: Sadara Chemical Company "
            "Location: Al Jubail, Saudi Arabia Product: Glycol ethers Capacity "
            "(tonnes/year): 200,000 Event start: 9 March Event finish:...",
            ["Chemicals"],
        ),
        (
            "MCM declares sales control on US methacrylic acid",
            "HOUSTON (ICIS)--Mitsubishi Chemical Methacrylates (MCM) has declared sales "
            "control on the following imported products in the US, according to a customer "
            "letter seen by ICIS: Methacrylic acid (MAA)...",
            ["Chemicals"],
        ),
        (
            "Opec+ boosts production in November",
            "",
            ["Oil - Crude"],
        ),
        (
            "China's CATL to raise $1.4bn to fund battery projects",
            "",
            ["Energy Transition"],
        ),
        (
            "Viewpoint: Japan to advance biocarbon usage",
            "",
            ["Energy Transition"],
        ),
        (
            "Singapore's carbon tax serves as key pillar of climate strategy",
            "",
            ["Energy Transition"],
        ),
        (
            "Australia's final carbon leakage review recommends CBAM-like scheme for high-risk sectors",
            "",
            ["Energy Transition"],
        ),
        (
            "ME ammonia futures rise to $590/t fob for April",
            "",
            ["Fertilizers"],
        ),
        (
            "Plant status: Thailand’s PTTGC shuts LDPE line for maintenance",
            "SINGAPORE (ICIS)--Here is a plant status report: Name: PTT Global Chemical "
            "(PTTGC) Location: Map Ta Phut, Thailand Products: Low density polyethylene "
            "(LDPE) Capacity (tonnes/year): 380,000 Event...",
            ["Chemicals"],
        ),
        (
            "Gas to remain in Germany’s residential demand mix amid proposed heating law reset",
            "Additional reporting by Eduardo Escajadillo Germany plans to scrap its "
            "heating mandate and let households keep choosing gas/oil Residential gas "
            "demand is likely to stay stable, as decarbonization...",
            ["Natural Gas", "Energy Transition"],
        ),
        (
            "How the US-Iran war is reshaping polymers trade routes in the Middle East",
            "DUBAI (ICIS)--Since the start of the war between the US and Iran in the "
            "Middle East, exports from Gulf Cooperation Council (GCC)-based suppliers "
            "have become increasingly more complex, and costlier....",
            ["Chemicals", "Shipping"],
        ),
        (
            "European ammonia production costs exceed imports as gas prices surge",
            "The cost of domestic European ammonia production has outstripped the cost of "
            "imported tons for the first time since June 2025, tracking soaring natural "
            "gas prices, data from Platts showed.",
            ["Fertilizers", "Natural Gas"],
        ),
        (
            "South Korea sees no LNG shortages despite Middle East supply disruptions",
            "South Korea said it sees no LNG shortages despite a production halt in Qatar "
            "and disruptions to transit through the Strait of Hormuz, as the country has "
            "sufficient stockpiles to buffer any short-term interruptions while preparing "
            "to secure alternative supplies.",
            ["LNG"],
        ),
        (
            "FEATURE: Hyperscalers continue to dominate corporate renewables contracts in 2025",
            "Hyperscalers continued to significantly outpace other industries in clean "
            "energy investments in 2025, but are expanding beyond traditional renewable "
            "energy contracts as the artificial intelligence race drives rising "
            "electricity demand.",
            ["Energy Transition"],
        ),
        (
            "Risk of prolonged conflict could leave Germany switching to coal, power imports amid unprofitable CCGTs",
            "Gas could play less of a role in the German power mix if gas prices remain "
            "high. With French exports already at capacity, a prolonged closure of the "
            "Strait of Hormuz could force Germany to consider...",
            ["Electric Power", "Coal"],
        ),
        (
            "India's crude throughput remains stable in January",
            "",
            ["Oil - Refined Products"],
        ),
        (
            "Thailand's Rayong Olefins declares FM on supplies",
            "SINGAPORE (ICIS)--Thailand's Rayong Olefins has declared force majeure "
            "(FM) on petrochemical supplies citing challenges with feedstock and "
            "logistics due to the ongoing conflict between US-Israel and...",
            ["Chemicals"],
        ),
        (
            "Europe olefins: tight supply, higher costs, more demand uncertainties due to Middle East conflict",
            "LONDON (ICIS)--European olefins supply is tight, and the spring planned "
            "turnaround season is about to get underway. Energy and logistics costs "
            "have ramped up significantly in the wake of the Middle...",
            ["Chemicals"],
        ),
        (
            "INSIGHT: Global BD prices jump on Middle East conflict; feedstock shortages in Asia",
            "LONDON (ICIS)--Global butadiene (BD) spot prices are climbing in March "
            "due to fallout from the US-Iran conflict. Rising crude prices and "
            "naphtha shortages led to a reduction in cracker operating...",
            ["Chemicals"],
        ),
        (
            "LyondellBasell declares Europe polyolefins force majeure",
            "LONDON (ICIS)--LyondellBasell has declared force majeure on polyolefins "
            "sales from two of the producer’s European subsidiaries, citing the "
            "uncertainty and market volatility brought on by the...",
            ["Chemicals"],
        ),
        (
            "Japan’s Idemitsu builds chemical recycling plant",
            "",
            ["Energy Transition"],
        ),
        (
            "USDA confirms soybean sales to China",
            "",
            ["Agriculture"],
        ),
        (
            "More than 30 sanctioned tankers in Venezuela",
            "",
            ["Shipping"],
        ),
        (
            "Calif. refiners warn emission rules risk closures",
            "",
            ["Oil - Refined Products"],
        ),
        (
            "NNPC upstream output at multi-year high on 1 December",
            "",
            ["Oil - Crude"],
        ),
        (
            "Turkey secures barley at prices below initial tender",
            "",
            ["Agriculture"],
        ),
        (
            "Al price forecasts hit $4,000/t on Middle East conflict",
            "",
            ["Metals"],
        ),
        (
            "ICIS EXPLAINS: Iran's political future to redefine oil and gas supply outlook",
            "Oil and gas prices have soared since the US and Israel launched a wave of "
            "attacks on Iran on 28th February. As Iran retaliates, striking critical "
            "energy installations across the region, the question...",
            ["Oil - Crude", "Natural Gas"],
        ),
        (
            "Praj Industries sees protein, SAF as dual growth drivers for global ethanol producers",
            "India's bioenergy company, Praj Industries, is positioning protein "
            "co-products as a key driver of future revenue growth for grain-based "
            "ethanol producers globally. Distillers are increasingly integrating "
            "protein-rich co-products for applications in animal feed as well as "
            "human nutrition.",
            ["Energy Transition", "Agriculture"],
        ),
        (
            "Strait of Hormuz closure threatens 40,000 t/month copper cathode flows to Gulf as Jebel Ali blocked, Khor Fakkan and Fujairah near capacity",
            "Roughly 40,000 tonnes per month of copper cathode that once flowed "
            "smoothly into the UAE through Jebel Ali had few options to reroute after "
            "the Strait of Hormuz officially closed, with the only alternative entry "
            "points — Khor Fakkan and Fujairah — already straining under the weight "
            "of diverted cargo.",
            ["Metals", "Shipping"],
        ),
        (
            "Africa PP prices up on closure of Strait of Hormuz and damage to oil and LNG operations",
            "LONDON (ICIS)--Africa polypropylene (PP) homopolymer raffia values "
            "gained 12.7-14.5% and PP copolymer prices rose by 12.8-15.8% this week "
            "in Africa. The markets saw first offers and deals concluded...",
            ["Chemicals"],
        ),
        (
            "G7 tankers gain share in Russia before US waiver for sanctioned ships",
            "A temporary US waiver has enabled sanctioned tankers to transport "
            "Russian oil, after G7-linked tanker operators increased their share in "
            "the restricted market last month. Russian crude and refined products "
            "loaded by tankers before March 5 could be sold to India...",
            ["Shipping", "Oil - Crude"],
        ),
        (
            "ET Highlights: Ammonia producers lead blue hydrogen, US states sue over hydrogen hubs, Japan's NEDO backs Kawasaki's liquid hydrogen projects",
            "Energy transition highlights: Our editors and analysts bring you the "
            "biggest stories from the industry this week, from renewables to storage "
            "to carbon prices.",
            ["Energy Transition"],
        ),
        (
            "Ammonia producers take lead on global blue hydrogen development",
            "The oil and gas industry believed that producing hydrogen and its "
            "derivatives could fill new markets for low-emission fuel. But fertilizer "
            "companies are competing for the same limited customer base — and so far, "
            "with greater success, according to industry watchers.",
            ["Energy Transition", "Fertilizers"],
        ),
        (
            "Plant status: S Korea's S-Oil commences No 2 RFCC planned turnaround on 6 Mar",
            "SINGAPORE (ICIS)--Here is a plant status report: Name: S-Oil Corp "
            "Location: Onsan, South Korea Product: Propylene Capacity "
            "(tonnes/year): 705,000 Event start: 6 March 2026 Event finish: "
            "H2 April 2026...",
            ["Chemicals", "Oil - Refined Products"],
        ),
        (
            "LNG share of alternative bunker fuel market grows",
            "",
            ["Shipping", "LNG"],
        ),
        (
            "FACTBOX: Fresh attacks hit shipping, energy operations in the Middle East",
            "Oil producers and exporters in the Middle East continue to face severe "
            "restrictions on shipping through the Strait of Hormuz and ongoing attacks "
            "on energy infrastructure. The scale and duration of a price spike will "
            "depend on how much oil is kept off the market due to danger in the "
            "strait, higher shipping insurance rates or damaged Gulf infrastructure.",
            ["Shipping", "Oil - Crude"],
        ),
        (
            "SE Asia REDD+ prices rise on supply crunch as new issuances loom large",
            "Persistent supply bottlenecks in Southeast Asia's REDD+ market have left "
            "buyers awaiting long-anticipated fresh issuances that could ease prices "
            "in the medium term.",
            ["Energy Transition"],
        ),
        (
            "Marine insurers seek solutions as Gulf shipping threats evolve",
            "Marine insurers are exploring new ways to revive seaborne trades in the "
            "Persian Gulf as threats shift from the weapons of past tanker wars to "
            "drones and modern missiles. More than 10 ships have been targeted in the "
            "Strait of Hormuz and surrounding waters.",
            ["Shipping"],
        ),
        (
            "INSIGHT: How US-Iran tension may reroute Asia polyolefin trade and where US exports fit",
            "SINGAPORE (ICIS)--The tensions between the US and Iran have disrupted the "
            "Middle Eastern energy and petrochemical supply chains. For Asian "
            "polyolefin markets, the risk is more about physical supply...",
            ["Chemicals"],
        ),
        (
            "N America chemical rail traffic rises for fourth week, US up 8.6%",
            "HOUSTON (ICIS)--North American chemical railcar traffic rose 5.3% year on "
            "year, to 51,419 loadings for the week ended 28 February, marking a fourth "
            "consecutive increase, according to freight rail...",
            ["Chemicals"],
        ),
        (
            "Singapore's Aster declares force majeure on MPG",
            "SINGAPORE (ICIS)--Singapore's Aster Chemicals and Energy declared on 6 "
            "March a force majeure on Monopropylene Glycol (MPG), according to market "
            "sources. This decision was made due to disruptions in...",
            ["Chemicals"],
        ),
        (
            "Venture Global nears CP2 Phase 2 FID",
            "VG expects CP2 Phase 2 FID in the coming weeks. New five-year 0.5mtpa "
            "deal with Trafigura starting Q2 2026.",
            ["LNG"],
        ),
        (
            "PSV traders holding off positioning on summer products as conflict continues",
            "PSV traders hesitant amid uncertainty over conflict duration, supply risk "
            "and delayed decree rollout.",
            ["Natural Gas"],
        ),
        (
            "India sets 2026-27 budget for P, K subsidy",
            "",
            ["Fertilizers"],
        ),
        (
            "US biofuel tax rule to benefit resellers, farmers",
            "",
            ["Energy Transition"],
        ),
        (
            "STJ endossa Cbios, mas estoques limitam preços",
            "",
            ["Energy Transition"],
        ),
        (
            "Viewpoint: Latam looks at domestic markets to spur H2",
            "",
            ["Energy Transition"],
        ),
        (
            "Iran conflict disrupts agricultural commodity flows; South American soymeal washouts reported",
            "The war between Israel, the United States and Iran is already affecting "
            "the flow of agricultural commodities from South America to Iran, "
            "particularly feed, with some soymeal cargoes said to have been washed "
            "out.",
            ["Agriculture"],
        ),
        (
            "FUJAIRAH DATA: Oil products stocks rise before FOIZ fire",
            "Oil products inventories at Fujairah in the UAE rose 1.3% in the week "
            "ended March 2 from the previous week, according to Fujairah Oil Industry "
            "Zone data published March 4.",
            ["Oil - Refined Products"],
        ),
        (
            "FACTBOX: Middle East uncertainty causes significant increases in chemical markets",
            "Several US chemical markets reached pricing levels not seen since last "
            "year, aligning themselves to the upward trend worldwide caused by the "
            "geopolitical uncertainty stemming from the war in the Middle East. "
            "Participants continued to increase the risk premium associated with the "
            "current situation, following steep reactions in the upstream crude oil "
            "sector.",
            ["Chemicals"],
        ),
        (
            "Caustic soda freight costs rise on Mideast Gulf war",
            "",
            ["Shipping", "Chemicals"],
        ),
        (
            "War pushes crude tankers to record highs",
            "",
            ["Shipping", "Oil - Crude"],
        ),
        (
            "Rotterdam biomarine 2025 sales fall as bio-LNG surges",
            "",
            ["Energy Transition", "LNG"],
        ),
        (
            "Iran says targeting Western-linked ships at Hormuz, denies full closure",
            "Iran has claimed it is restricting passage through the Strait of Hormuz "
            "only for vessels linked to Western nations and Israel rather than fully "
            "closing the critical energy chokepoint, as maritime traffic remained "
            "severely disrupted and regional oil exporters explored using stranded "
            "tankers for floating storage.",
            ["Shipping", "Oil - Crude"],
        ),
    ],
)
def test_classify_categories_regression_cases(
    title: str,
    description: str,
    expected: list[str],
) -> None:
    assert classify_categories(title, description) == expected


@pytest.mark.parametrize(
    ("title", "description"),
    [
        (
            "EVENING SNAPSHOT - Asia Markets Summary",
            "CRUDE: Lower after US President Donald Trump indicated Iran war may end soon "
            "and oil sanctions may ease. NAPHTHA (CFR JAPAN): Down tracking crude losses. "
            "BENZENE (FOB KOREA): Down alongside crude....",
        ),
        (
            "INTERACTIVE: Energy facilities and shipping hit in the Middle East war",
            "Iran has targeted energy infrastructure across the Middle East in response to "
            "attacks by the US and Israel launched Feb. 28. Here are some of the key oil, "
            "gas, shipping and chemicals assets affected by direct strikes and elevated "
            "security risks.",
        ),
        (
            "Americas top stories: weekly summary",
            "Here are the top stories from ICIS News from the week ended 6 March. "
            "INSIGHT: Strait of Hormuz chokepoint to shut in Middle East chemicals and "
            "plastics exports.",
        ),
        (
            "COMMODITY TRACKER: 6 charts to watch this week",
            "Global blue hydrogen capacity expands as fertilizer companies drive "
            "development. Meanwhile, Indian methanol and Thai rice prices decline amid "
            "shifting market conditions.",
        ),
        (
            "FACTBOX: Middle East war raises farm-to-fork food inflation risks on fuel, freight, fertilizer disruptions",
            "The Middle East war is raising the risk of food inflation following the "
            "upward trajectory of fuel, freight and fertilizer markets.",
        ),
    ],
)
def test_broad_roundups_abstain_and_normalize_to_general(
    title: str,
    description: str,
) -> None:
    assert classify_categories(title, description) == []

    article = {
        "title": title,
        "description": description,
        "category": "General",
        "categories": ["General"],
    }
    result = normalize_article_categories(article)

    assert result["categories"] == ["General"]
    assert result["used_classifier"] is False


def test_classify_category_public_api_still_returns_joined_string() -> None:
    assert (
        classify_category(
            "Gas to remain in Germany’s residential demand mix amid proposed heating law reset",
            "Germany plans to scrap its heating mandate and let households keep choosing "
            "gas/oil. Residential gas demand is likely to stay stable, as "
            "decarbonization...",
        )
        == "Natural Gas, Energy Transition"
    )


def test_explain_classification_returns_keyword_and_score_provenance() -> None:
    debug = explain_classification(
        "Italian gas-grid operator unfazed by seasonal backwardation ahead of injection season",
        "Still time to support gas injection, CEO maintains.",
    )

    assert debug["categories"] == ["Natural Gas"]
    assert debug["scores"]["Natural Gas"]["title_score"] > 0
    assert any(
        match["keyword"] == "injection season"
        and match["field"] == "title"
        and match["category"] == "Natural Gas"
        for match in debug["matches"]
    )


def test_normalize_overrides_stale_informative_source_label() -> None:
    article = {
        "title": "Singapore's carbon tax serves as key pillar of climate strategy",
        "description": "",
        "category": "Fertilizers",
        "categories": ["Fertilizers"],
    }

    result = normalize_article_categories(article)

    assert result["categories"] == ["Energy Transition"]
    assert result["used_classifier"] is True


def test_normalize_reconciles_wrong_existing_dual_labels() -> None:
    article = {
        "title": "Gas to remain in Germany’s residential demand mix amid proposed heating law reset",
        "description": "Germany plans to scrap its heating mandate and let households keep "
        "choosing gas/oil. Residential gas demand is likely to stay stable, as "
        "decarbonization...",
        "category": "Energy Transition",
        "categories": ["Energy Transition", "Metals"],
    }

    result = normalize_article_categories(article)

    assert result["categories"] == ["Natural Gas", "Energy Transition"]
    assert result["used_classifier"] is True


def test_normalize_can_augment_existing_single_label_with_strong_second_category() -> None:
    article = {
        "title": "How the US-Iran war is reshaping polymers trade routes in the Middle East",
        "description": "Exports from Gulf Cooperation Council-based suppliers have become "
        "increasingly more complex and costlier.",
        "category": "Chemicals",
        "categories": ["Chemicals"],
    }

    result = normalize_article_categories(article)

    assert result["categories"] == ["Chemicals", "Shipping"]
    assert result["used_classifier"] is True


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("european lng imports rise", ["LNG"]),
        ("european mdi imports rise", ["Chemicals"]),
        ("european cbam plan advances", ["Energy Transition"]),
        ("vlcc rates jump", ["Shipping"]),
        ("asia abs demand improves", ["Chemicals"]),
        ("asia pe demand weakens", ["Chemicals"]),
        ("asia pp demand improves", ["Chemicals"]),
        ("global bd supply tightens", ["Chemicals"]),
        ("asia pp prices rise", ["Chemicals"]),
        ("asia abs market rises", ["Chemicals"]),
        ("global bd prices jump", ["Chemicals"]),
        ("china pe market rises", ["Chemicals"]),
        ("uk ccgt margins improve", ["Electric Power"]),
    ],
)
def test_lowercase_acronym_matching_is_supported_where_safe(
    title: str,
    expected: list[str],
) -> None:
    assert classify_categories(title, "") == expected
