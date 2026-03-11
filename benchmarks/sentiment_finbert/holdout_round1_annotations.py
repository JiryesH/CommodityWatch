"""Manual annotations for the first unseen sentiment holdout round."""

from __future__ import annotations


def entry(
    title_label: str,
    title_rationale: str,
    *,
    title_ambiguity: bool = False,
    title_description_label: str | None = None,
    title_description_rationale: str | None = None,
    title_description_ambiguity: bool = False,
    notes: str = "",
) -> dict[str, object]:
    return {
        "title": {
            "label": title_label,
            "ambiguity": title_ambiguity,
            "rationale": title_rationale,
        },
        "title_description": {
            "label": title_description_label or title_label,
            "ambiguity": title_description_ambiguity,
            "rationale": title_description_rationale or title_rationale,
        },
        "notes": notes,
    }


ANNOTATIONS: dict[str, dict[str, object]] = {
    # bullish
    "dd89d051770f8cbc": entry(
        "positive",
        "Oil price surge is bullish for crude.",
        title_ambiguity=True,
        title_description_rationale="Description confirms higher crude on supply disruption.",
    ),
    "33f86ffbd1128063": entry(
        "positive",
        "Tight supply and higher prices are bullish.",
    ),
    "171c33383f049a8e": entry(
        "positive",
        "Government boost to EV sales is supportive.",
    ),
    "4bf47f3387fe4d1c": entry(
        "positive",
        "Higher biomass co-firing is supportive adoption growth.",
        title_ambiguity=True,
    ),
    "ed1a13abc779fe38": entry(
        "positive",
        "Tighter European gas balance is bullish for gas.",
        title_ambiguity=True,
    ),
    "7c926ec89ed1a7a8": entry(
        "neutral",
        "Monthly rebound but yearly decline is mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description keeps the rebound and demand headwinds mixed.",
    ),
    "ba956ded8240a2c6": entry(
        "positive",
        "Braskem running dry implies tighter PE supply.",
    ),
    "6e192f69a56e2fd4": entry(
        "neutral",
        "Import surge is factual without clear market direction.",
        title_ambiguity=True,
    ),
    "cde67e431c08a876": entry(
        "positive",
        "Sharp propane gain is bullish for pricing.",
        title_ambiguity=True,
    ),
    "dcf4b0bc10b9d53d": entry(
        "neutral",
        "Rely on support is not directionally clear.",
        title_ambiguity=True,
    ),
    "de0acbdcf535d962": entry(
        "positive",
        "Record utility capex is supportive investment growth.",
    ),
    "11eb37be325cc332": entry(
        "positive",
        "Spot surge on closure is bullish for VAM pricing.",
        title_ambiguity=True,
        title_description_rationale="Description confirms sharply higher spot pricing.",
    ),
    "a454c80bb1951f7b": entry(
        "positive",
        "Sales rise on strong demand is supportive.",
    ),

    # bearish
    "bc3c917fc5982709": entry(
        "negative",
        "Fewer diesel spot deals are harmful.",
    ),
    "cf8edb338f8cb6ad": entry(
        "negative",
        "Lower gas futures on mild weather are bearish.",
        title_description_rationale="Description confirms a weaker Henry Hub session.",
    ),
    "b35e3be949f8759a": entry(
        "negative",
        "Output quota cuts are harmful for coal producers.",
        title_ambiguity=True,
    ),
    "02ec2420ba0c4c3f": entry(
        "negative",
        "Cracker run cuts are a clear operational negative.",
        title_description_rationale="Description confirms reduced cracker runs.",
    ),
    "b666480c7e009a12": entry(
        "negative",
        "Lower crude prices are bearish for Canadian crude.",
    ),
    "56a29f3fe2398b3e": entry(
        "negative",
        "Structural industry decline is clearly negative.",
    ),
    "51a17254348263ca": entry(
        "negative",
        "Power-market slump is clearly negative.",
    ),
    "ef1653f19af37459": entry(
        "neutral",
        "Lower stocks but weaker sales offset each other.",
        title_ambiguity=True,
    ),
    "6dd0aadae3e09183": entry(
        "negative",
        "Lower run rates are a clear operational negative.",
        title_description_rationale="Description confirms the unit is running lower.",
    ),
    "fb3924094b2a79db": entry(
        "negative",
        "Cracker run cuts are a clear operational negative.",
        title_description_rationale="Description confirms reduced cracker runs.",
    ),
    "6d9e2a60169b6e71": entry(
        "negative",
        "Plant run-rate falls are a bearish operational signal.",
        title_description_rationale="Description confirms the lower average run rate.",
    ),
    "98077629ac47dc81": entry(
        "neutral",
        "A 2040 emissions target is policy detail, not direction.",
    ),
    "7caa39ac651fcfe7": entry(
        "negative",
        "High prices hurting demand is bearish.",
    ),

    # neutral
    "89a680e4cf5cfe41": entry(
        "negative",
        "Oil prices tumbling is bearish for crude.",
        title_description_rationale="Description confirms prices fell on easing supply fears.",
    ),
    "a5bd22bedb7e18fd": entry(
        "positive",
        "New projects reversing decline are supportive.",
        title_ambiguity=True,
        title_description_rationale="Description confirms the projects aim to reverse decline.",
    ),
    "e508058c21bfd5b1": entry(
        "positive",
        "Shut-in methanol exports imply tighter supply.",
        title_ambiguity=True,
        title_description_rationale="Description confirms major supply is constrained.",
    ),
    "2d129b75233e59d6": entry(
        "neutral",
        "Policy move on a spread is not directionally clear.",
    ),
    "3f38a076a8146cf7": entry(
        "neutral",
        "Weekly summary headline is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description is a multi-story summary with mixed signals.",
    ),
    "ade986d88da12200": entry(
        "positive",
        "Launching an energy-transition initiative is supportive progress.",
    ),
    "0a9738256c401c75": entry(
        "negative",
        "Force majeure declaration is a clear operational negative.",
        title_description_rationale="Description confirms the force majeure event.",
    ),
    "5da517f96ce3b4aa": entry(
        "neutral",
        "Plans to ramp imports are still tentative.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description says Serbia is only in talks to ramp imports.",
    ),
    "7b2a61a690f27ee2": entry(
        "neutral",
        "Implementation update is procedural and factual.",
    ),
    "92182e88e77df5d9": entry(
        "neutral",
        "No reserve-release plans are factual without clear direction.",
        title_ambiguity=True,
    ),
    "0c60c92f106e2ed7": entry(
        "negative",
        "Force majeure on supplies is a clear negative.",
        title_description_rationale="Description confirms disrupted feedstock shipments.",
    ),
    "b4e95d1cab58ff3a": entry(
        "neutral",
        "Policy easing detail is not directionally clear.",
        title_ambiguity=True,
    ),
    "1c11e963761ede4e": entry(
        "positive",
        "Sanctions waivers improve trade conditions.",
        title_ambiguity=True,
    ),

    # geopolitical
    "3a3d0d0b764f1de9": entry(
        "negative",
        "Potential force majeure is a harmful operational signal.",
        title_description_rationale="Description confirms the warning stems from blockade risk.",
    ),
    "5a443bf1d45cf71e": entry(
        "negative",
        "Force majeure on supplies is a clear negative.",
        title_description_rationale="Description confirms the force majeure event.",
    ),
    "14c00dfa33a31ee6": entry(
        "positive",
        "Offer spike on feedstock disruption is bullish pricing.",
        title_ambiguity=True,
        title_description_rationale="Description confirms higher offers and costs.",
    ),
    "ec7047873ff54aa9": entry(
        "negative",
        "Tariff dispute headline is trade-negative.",
    ),
    "51add781d0754360": entry(
        "positive",
        "Tariff refunds relieve trade costs.",
    ),
    "859a1d885aa4c399": entry(
        "positive",
        "Ports resuming operations is supportive normalization.",
    ),
    "7ebbabf49a3103a6": entry(
        "neutral",
        "Directive to maximize LPG output is policy action, not clear direction.",
        title_description_ambiguity=True,
    ),
    "de71432909bf8241": entry(
        "negative",
        "Export disruption is a clear negative trade signal.",
        title_description_rationale="Description confirms vessels are avoiding Hormuz.",
    ),
    "924b6acfaefa0fbf": entry(
        "negative",
        "Buyers bracing for price hikes is harmful for buyers.",
        title_description_rationale="Description confirms early conflict-driven price pressure.",
    ),
    "2224c5eebece53bf": entry(
        "positive",
        "Mediation to end war is de-escalatory and supportive.",
        title_ambiguity=True,
    ),
    "5b39ed85c37080a5": entry(
        "neutral",
        "Analyst view on limited LNG scope is analytical and mixed.",
        title_description_ambiguity=True,
    ),
    "46d7cc9442c02614": entry(
        "negative",
        "Biodiesel premiums falling is bearish for premiums.",
        title_description_rationale="Description confirms premiums fell sharply.",
    ),
    "6deb0733c1f1b597": entry(
        "positive",
        "Headline explicitly says the methanol market turns bullish.",
        title_description_rationale="Description confirms bullish spot pricing on supply risks.",
    ),

    # outage
    "34dd8f0e12f48181": entry(
        "negative",
        "Food inflation risk from disruptions is harmful.",
        title_description_rationale="Description confirms rising inflation risk.",
    ),
    "9e24d8e83b675652": entry(
        "neutral",
        "Commodity tracker headline is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description spans multiple commodities and mixed signals.",
    ),
    "f13c89b1f3a7fc26": entry(
        "negative",
        "Overhaul of EVA units is a maintenance disruption.",
        title_description_rationale="Description confirms scheduled overhaul.",
    ),
    "918088b2f950e3c3": entry(
        "negative",
        "Cutting cracker runs is a clear operational negative.",
        title_description_rationale="Description confirms reduced cracker runs.",
    ),
    "6bb76c0c1f9c4af8": entry(
        "negative",
        "Cutting cracker runs is a clear operational negative.",
        title_description_rationale="Description confirms squeezed feedstock concerns.",
    ),
    "92ce23a3e4c38c64": entry(
        "neutral",
        "Snapshot headline is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description mixes upside bias with temporary-disruption framing.",
    ),
    "713471840e9c435f": entry(
        "negative",
        "Force majeure on PET supply is a clear negative.",
        title_description_rationale="Description confirms the FM declaration.",
    ),
    "cf63de8f424ee3ef": entry(
        "positive",
        "Higher run rate on restarts is supportive.",
        title_description_rationale="Description confirms run rate increased on restarts.",
    ),
    "2b29fb37a33b9b08": entry(
        "negative",
        "Planned strike is a clear disruption risk.",
    ),
    "adea7de280bf639e": entry(
        "negative",
        "Catastrophic halt wording is clearly harmful.",
    ),
    "ef4404f7b84aee2a": entry(
        "positive",
        "PE market lifted on cost and import disruption is bullish pricing.",
        title_ambiguity=True,
        title_description_rationale="Description confirms PE prices surged.",
    ),
    "a72db6a3de310262": entry(
        "neutral",
        "Restart plus possible run cuts makes the headline mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "53eec76d7ac283f1": entry(
        "negative",
        "Force majeure at a plant is a clear negative.",
        title_description_rationale="Description confirms the FM declaration.",
    ),

    # macro
    "0d3bcb89c20a936c": entry(
        "positive",
        "Reassurance of adequate fertilizer stocks is supportive.",
        title_ambiguity=True,
        title_description_rationale="Description confirms inventories are secure.",
    ),
    "30ead8b1f795a77d": entry(
        "neutral",
        "GDP harm and competitiveness boost offset each other.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "ea33b9fb7aaa58de": entry(
        "neutral",
        "Court update on quota timing is procedural.",
    ),
    "130de83c9e7bb443": entry(
        "positive",
        "Oil supply buffers cushioning risk is reassuring.",
        title_ambiguity=True,
    ),
    "535f6e6089bf812c": entry(
        "negative",
        "Broad tariff threat is trade-negative.",
    ),
    "331881c971705874": entry(
        "neutral",
        "Viewpoint on tax credits is analytical, not directional.",
    ),
    "b0a55e8ac02a68a9": entry(
        "neutral",
        "Inflation and election context are macro facts, not direction.",
    ),
    "015c029e077f4758": entry(
        "positive",
        "Higher gas price forecast is bullish for gas pricing.",
        title_ambiguity=True,
    ),
    "e1bc44d5aea05455": entry(
        "neutral",
        "Inflation acceleration is macro data without clear commodity direction.",
    ),
    "76c3ca83fec1398b": entry(
        "neutral",
        "CBAM impact consideration is policy analysis only.",
    ),
    "c38f884ead0bfe26": entry(
        "neutral",
        "Three market pressures is explicitly mixed framing.",
        title_description_ambiguity=True,
    ),
    "77b4edf273a82ba3": entry(
        "neutral",
        "Output boost and war-driven supply threat offset each other.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "4203f73c44f4816e": entry(
        "neutral",
        "Vote scheduling on GHG targets is procedural.",
    ),

    # description_needed
    "f69b38052f54f2a0": entry(
        "positive",
        "Removing oil sanctions improves trade conditions.",
        title_description_rationale="Description confirms sanctions relief on some countries.",
    ),
    "a9e97d03cb182b7d": entry(
        "neutral",
        "Storage auction headline is factual and not market-directional.",
        title_description_ambiguity=True,
    ),
    "bb0f11d9f5750abe": entry(
        "negative",
        "Force majeure declaration is a clear operational negative.",
        title_description_rationale="Description confirms the PX force majeure.",
    ),
    "4e1893a7e5954a90": entry(
        "neutral",
        "Topic-page update title is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description is an update wrapper, not a single directional claim.",
    ),
    "3509fb4d5978a95a": entry(
        "negative",
        "Falling caustic soda margins are bearish.",
        title_description_rationale="Description confirms margins fell as energy costs rose.",
    ),
    "7a492350ca388290": entry(
        "positive",
        "Benzene rally resuming with buyers scrambling is bullish.",
        title_description_rationale="Description confirms stronger benzene pricing.",
    ),
    "c2b35268a8770a10": entry(
        "neutral",
        "Dependency-risk analyst view is analytical and mixed.",
        title_description_ambiguity=True,
    ),
    "983f811f09101b09": entry(
        "neutral",
        "Snapshot headline is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description is a multi-market summary with mixed moves.",
    ),
    "1ecc2f10057d6567": entry(
        "positive",
        "Crude prices climbing on supply-chain threats is bullish.",
        title_ambiguity=True,
        title_description_rationale="Description confirms higher crude on supply risk.",
    ),
    "0eff4a93cb4f1d09": entry(
        "neutral",
        "News wrap headline is a broad vulnerability summary.",
        title_description_ambiguity=True,
    ),
    "112bdbf3edd24ccc": entry(
        "neutral",
        "Weekly summary headline is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description summarizes several markets with mixed moves.",
    ),
    "7ed04c5f08336d52": entry(
        "neutral",
        "Snapshot headline is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description is a multi-market summary with mixed moves.",
    ),
    "40781579eff9b06f": entry(
        "positive",
        "Oil futures crossing $110 on production hits is bullish.",
        title_ambiguity=True,
        title_description_rationale="Description confirms oil futures extended gains on supply risk.",
    ),

    # ambiguous
    "de8409e21d80f9b8": entry(
        "neutral",
        "Quarterly settled price is factual and not directional.",
    ),
    "15889d08180f5fd5": entry(
        "neutral",
        "Loading is complicated but operations continue, so mixed.",
        title_ambiguity=True,
    ),
    "dd41711939167b4f": entry(
        "positive",
        "Oil deficit risk is bullish for crude.",
        title_ambiguity=True,
    ),
    "d1311790fd83f31f": entry(
        "neutral",
        "Missed opportunity to reduce prices is analytical and mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "df2abebdf88c6b0f": entry(
        "neutral",
        "War duration estimate is factual without clear direction.",
    ),
    "d5190ce9c9773ee0": entry(
        "neutral",
        "Stocks rising is factual without clear market direction.",
        title_ambiguity=True,
    ),
    "ca2f6b62c77500f0": entry(
        "neutral",
        "Uncertain security timeline is procedural and unclear.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "c7043d1222be724c": entry(
        "neutral",
        "Considering discretionary blending is still tentative policy.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "7ad0c7cb38a2f236": entry(
        "negative",
        "Expansion that adds length and pressure is bearish.",
        title_description_rationale="Description confirms pressure on other VCM producers.",
    ),
    "49e4cce73d41a963": entry(
        "neutral",
        "Q&A on redefining imports is analytical and non-directional.",
    ),
    "f8cc06c266a30063": entry(
        "positive",
        "Inventories falling with rising buying is bullish.",
        title_description_rationale="Description confirms stronger buying and lower inventories.",
    ),
    "802a3c9c56b6b2e0": entry(
        "positive",
        "Potential flat-steel recovery is supportive.",
        title_ambiguity=True,
    ),
    "fac8b8bd8d884b9c": entry(
        "neutral",
        "Restarts offsetting shortfalls but gas risk remains is mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
}
