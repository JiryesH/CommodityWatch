"""Manual sentiment annotations for the FinBERT benchmark sample."""

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
    "e782370f93ff3872": entry(
        "neutral",
        "Cross-border share shift is mixed and speculative.",
        title_ambiguity=True,
    ),
    "7037e11347205739": entry(
        "positive",
        "Possible PP supply tightening is bullish for PP.",
        title_ambiguity=True,
        title_description_rationale="Description confirms tighter PP supply risk.",
    ),
    "9b886264f2f0efb7": entry(
        "neutral",
        "Refinery price hike is factual without clear net benefit.",
        title_ambiguity=True,
    ),
    "b3d46c31d83f6692": entry(
        "positive",
        "New investment surge improves project economics.",
        title_description_rationale="Description confirms resumed auctions and stronger investment outlook.",
    ),
    "245774e6ccbf4231": entry(
        "positive",
        "Premiums surging indicates stronger B7 pricing.",
        title_ambiguity=True,
    ),
    "9b393262f308e1a6": entry(
        "positive",
        "Rallying offers point to tighter toluene conditions.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description shows extended gains despite demand uncertainty.",
    ),
    "167d3b4b34126f46": entry(
        "positive",
        "Oil price surge is directionally bullish.",
        title_ambiguity=True,
        title_description_rationale="Description says gains and buying interest extended.",
    ),
    "733e136b8043e682": entry(
        "neutral",
        "Higher demand and supply estimates offset each other.",
        title_ambiguity=True,
        title_description_label="positive",
        title_description_rationale="Description adds demand strength and supply below demand.",
        title_description_ambiguity=True,
    ),
    "4e08e1de93350661": entry(
        "positive",
        "Higher solvents prices imply firmer market conditions.",
        title_ambiguity=True,
        title_description_rationale="Description ties price strength to costs and supply restraints.",
    ),
    "d1718ebfa5077559": entry(
        "positive",
        "Base oils price increase is directionally bullish.",
        title_ambiguity=True,
    ),
    "67d99fb53ae46aa6": entry(
        "positive",
        "Potential boost improves refiners' near-term outlook.",
        title_ambiguity=True,
    ),
    "ad7402b4f10dd5c1": entry(
        "negative",
        "More Opec+ supply is bearish for crude prices.",
    ),
    "d202cec1e950984a": entry(
        "positive",
        "Higher output and grade improve the producer outlook.",
        title_ambiguity=True,
    ),
    "1264107c19403d27": entry(
        "positive",
        "Rebounding steel demand is supportive.",
    ),

    # bearish
    "abe34cfdfa38b990": entry(
        "neutral",
        "Lower CBAM benchmarks are policy detail with unclear direction.",
        title_ambiguity=True,
    ),
    "82c1ec192461a180": entry(
        "negative",
        "Under-pressure wording signals harmful market strain.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description still centers on conflict risk and logistical strain.",
    ),
    "978e249724f84583": entry(
        "negative",
        "Futures slumping on tumbling oil is clearly bearish.",
        title_description_rationale="Description confirms broad futures weakness.",
    ),
    "95998dfe96a64afc": entry(
        "negative",
        "Under pressure implies weaker recycled polymer conditions.",
    ),
    "059331ed5f46c85b": entry(
        "neutral",
        "Higher crude helps oil but hurts styrene chains, so mixed.",
        title_ambiguity=True,
        title_description_label="negative",
        title_description_rationale="Description focuses on upstream cost pressure on styrene derivatives.",
    ),
    "3b1c25a6b0330dda": entry(
        "neutral",
        "Split market and panic buying are explicitly mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description shows producer hikes and buyer resistance offsetting.",
    ),
    "0022786ece7e3ddd": entry(
        "negative",
        "Oversupply and lower prices are clearly bearish.",
    ),
    "c64d7b0f0fecf054": entry(
        "neutral",
        "Lower imports alone are not directionally meaningful.",
        title_ambiguity=True,
    ),
    "936b161da98ea573": entry(
        "negative",
        "Cutting cracker runs and force majeure are harmful events.",
        title_description_rationale="Description confirms operational curtailment.",
    ),
    "ece4a10f60c2e14f": entry(
        "neutral",
        "War support and weather pressure pull in opposite directions.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description remains mixed across opposing power drivers.",
    ),
    "ebb3dc5e3a22e4d0": entry(
        "negative",
        "Zero gas flows indicate a supply disruption.",
        title_description_rationale="Description confirms the interconnector outage.",
    ),
    "93ac0f1ecca5debf": entry(
        "negative",
        "Output and export declines are harmful for the sector.",
    ),
    "22d10fcad145a453": entry(
        "negative",
        "Cracker run cuts signal operational weakness.",
        title_description_rationale="Description confirms reduced operating rates.",
    ),
    "ffab92c133377e15": entry(
        "negative",
        "Run-rate cuts on feedstock disruption are clearly harmful.",
        title_description_rationale="Description confirms sustained supply disruption.",
    ),

    # neutral
    "204907afad73ab5b": entry(
        "neutral",
        "Political backlash around a plan is not directionally clear.",
        title_ambiguity=True,
    ),
    "7a9df10583fe2eef": entry(
        "neutral",
        "Policy-clarity Q&A is analytical, not directional.",
    ),
    "bcb440899e203dae": entry(
        "neutral",
        "Snapshot title alone is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Summary mixes crude strength with other market moves.",
    ),
    "394954fd8037c54a": entry(
        "positive",
        "Capacity enhancement plans improve the facility outlook.",
        title_description_rationale="Description confirms planned expansion and optimization.",
    ),
    "4be1adea8cdb3c72": entry(
        "neutral",
        "MoU signing is factual and not market directional.",
    ),
    "472b24cb347ebb96": entry(
        "negative",
        "Delayed tariff refunds are harmful to affected traders.",
    ),
    "85888b74ece064ec": entry(
        "positive",
        "Sharp PE price spikes indicate bullish local pricing.",
        title_ambiguity=True,
        title_description_rationale="Description ties the spike to import shortages.",
    ),
    "a09bb0d075a16010": entry(
        "neutral",
        "Possible quota waivers are only tentative policy talk.",
        title_ambiguity=True,
    ),
    "32eaf508ecf431d4": entry(
        "neutral",
        "Call for roadmaps is procedural and factual.",
    ),
    "05ca2a6ff2f094aa": entry(
        "positive",
        "Starting a second co-firing test is project progress.",
    ),
    "46dec6ea70e8ae32": entry(
        "neutral",
        "Planning a future FID is still tentative.",
        title_ambiguity=True,
    ),
    "60608124335345c2": entry(
        "neutral",
        "Takeover plans are factual without clear market direction.",
        title_ambiguity=True,
    ),
    "381c1e63888232ec": entry(
        "positive",
        "Soaring crude premiums are bullish for Mars pricing.",
        title_ambiguity=True,
    ),
    "94852195f706db36": entry(
        "neutral",
        "Phase-out options discussion is policy process, not direction.",
    ),

    # geopolitical
    "12cafb5ef3c1076f": entry(
        "neutral",
        "Waiting for clarity has no directional market signal.",
    ),
    "cadb561a08a7ef9d": entry(
        "neutral",
        "Snapshot title alone is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description summarizes several markets with mixed moves.",
    ),
    "ae9c71e6e95cef40": entry(
        "positive",
        "Upward pressure and price hikes are bullish for MDI.",
        title_ambiguity=True,
        title_description_rationale="Description confirms supply shocks and higher pricing pressure.",
    ),
    "1e2267fcd7d4ce84": entry(
        "neutral",
        "Firm spot prices but stalled contract is mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description keeps both firmer spot and stalled contract signals.",
    ),
    "81200c7ffa2e300a": entry(
        "neutral",
        "Oil tax cut is a policy response with unclear net impact.",
        title_ambiguity=True,
    ),
    "4c896ead51d91c5e": entry(
        "positive",
        "Lifting sanctions improves Belarus potash trade conditions.",
        title_ambiguity=True,
    ),
    "b6f168ff4364b7b4": entry(
        "positive",
        "Oil nearing $80 on halted transit is bullish for crude.",
        title_ambiguity=True,
    ),
    "019efd6b1cbdc8da": entry(
        "neutral",
        "Snapshot title alone is non-directional.",
        title_description_ambiguity=True,
        title_description_rationale="Description mixes crude gains with softer downstream signals.",
    ),
    "228abeec7a355d5e": entry(
        "neutral",
        "No plant damage yet is reassuring but shipping remains disrupted.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "fcfb106bfde3a925": entry(
        "positive",
        "Surging urea derivatives imply stronger pricing.",
        title_ambiguity=True,
    ),
    "178de683b894211c": entry(
        "negative",
        "Shipping woes and buyer concern are harmful signals.",
        title_description_rationale="Description centers on trade disruption concerns.",
    ),
    "bfb98ced3edeff1e": entry(
        "negative",
        "Stranded ammonia ships and halted traffic are clearly disruptive.",
        title_description_rationale="Description confirms shipping paralysis.",
    ),
    "51adf164dc538e3a": entry(
        "negative",
        "Forced crude output curbs are harmful for producers.",
    ),
    "5aa568899458a531": entry(
        "negative",
        "Supply uncertainty and operating-rate risk are harmful.",
        title_description_rationale="Description confirms feedstock disruption risk.",
    ),

    # outage
    "c643347f7dc67a5f": entry(
        "negative",
        "Halting a green ammonia project is clearly harmful.",
    ),
    "f202ec4399b1245e": entry(
        "negative",
        "Force majeure and surcharge signal operational stress.",
        title_description_rationale="Description confirms force majeure on PVC supply.",
    ),
    "2727fda59e711b58": entry(
        "negative",
        "Supply-chain vulnerability from war is harmful.",
        title_description_rationale="Description points to forced run cuts.",
    ),
    "948ce199fd6ad1a4": entry(
        "positive",
        "Restarting output improves operating conditions.",
    ),
    "0d75688ec3000a0c": entry(
        "negative",
        "Crude output cuts on disruption are harmful.",
    ),
    "fa376f39f63295c2": entry(
        "positive",
        "Resuming US crude imports would improve trade flows.",
        title_ambiguity=True,
        title_description_rationale="Description says refiners may resume imports despite tariffs.",
    ),
    "9752b41c6feba551": entry(
        "negative",
        "Halting oil supply is a clear disruption.",
    ),
    "a96c8f40664574db": entry(
        "negative",
        "Stopping BDO production is clearly harmful.",
        title_description_rationale="Description confirms the shutdown event.",
    ),
    "9cf1362fd3336d8d": entry(
        "neutral",
        "Wider pricing on rebalanced supply is not clearly directional.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description says disruption impact is limited and mixed.",
    ),
    "2c576c1876cd926c": entry(
        "negative",
        "Technical issues delaying start-up are harmful.",
        title_description_rationale="Description confirms delayed restart timing.",
    ),
    "1b60a36a1c8605d0": entry(
        "neutral",
        "Topic-page update headline is non-directional.",
    ),
    "aaaa5504cd92415e": entry(
        "negative",
        "Crude output cuts from disruption are harmful.",
    ),
    "a701d2c2130743a2": entry(
        "neutral",
        "Talks to buy a refinery remain tentative.",
        title_ambiguity=True,
    ),
    "079e4543de64b272": entry(
        "negative",
        "Plant shutdown on gas disruption is clearly harmful.",
        title_description_rationale="Description confirms the shutdown event.",
    ),

    # macro
    "02cdf15f18a8a025": entry(
        "negative",
        "Tariff threats are harmful trade signals.",
    ),
    "a2cba6c498919891": entry(
        "negative",
        "Ending Russian gas exports to Europe would be disruptive.",
        title_description_rationale="Description reinforces supply disruption risk.",
    ),
    "0eec7e4a523b996d": entry(
        "neutral",
        "A quarter-point Fed cut is macro news with unclear commodity direction.",
        title_ambiguity=True,
    ),
    "5308c80f4c04f0e7": entry(
        "neutral",
        "Review recommendation is policy commentary, not a market move.",
    ),
    "b7dc349f546343f8": entry(
        "neutral",
        "Maintaining an export tax is factual without clear direction.",
        title_ambiguity=True,
    ),
    "ecf9feb90fcda475": entry(
        "neutral",
        "Shrugging off the rollback implies little net market move.",
    ),
    "cbc39dd31d8174f5": entry(
        "neutral",
        "Opposition to tariff removal is policy positioning only.",
    ),
    "67fb50c8b71f6877": entry(
        "positive",
        "Lower utility tariffs would reduce power costs.",
    ),
    "c1fab2c27275152a": entry(
        "neutral",
        "Fed hold is routine macro information.",
    ),
    "6cf01b48e782c018": entry(
        "neutral",
        "A loophole headline is policy detail with unclear direction.",
        title_ambiguity=True,
    ),
    "dc71b1f3c5412d88": entry(
        "neutral",
        "Possible quota waivers are tentative and ambiguous.",
        title_ambiguity=True,
    ),
    "ace70893c5a9bf52": entry(
        "neutral",
        "Subsidy outline is policy detail without explicit direction.",
    ),
    "8b91a52184215209": entry(
        "positive",
        "Commissioning new shredding capacity is project progress.",
    ),
    "90aee91c5d6073f6": entry(
        "neutral",
        "Softer inflation is macro data, not a direct commodity signal.",
        title_ambiguity=True,
    ),

    # description_needed
    "54bf6f3d0b387a71": entry(
        "positive",
        "Deepening shortage implies tighter chicken pricing.",
        title_ambiguity=True,
        title_description_rationale="Description confirms tight supply and 6% price gains.",
    ),
    "fdd458918e6eb90c": entry(
        "negative",
        "Negative margins and import collapse are clearly bearish.",
        title_description_rationale="Description confirms weak demand and margin pressure.",
    ),
    "6982d15a27f92a20": entry(
        "positive",
        "Bracing for hikes implies firmer AA pricing.",
        title_ambiguity=True,
        title_description_rationale="Description confirms price-rise pressure from conflict.",
    ),
    "327532c1edc3ed59": entry(
        "positive",
        "Gas futures rising after a withdrawal is bullish.",
        title_description_rationale="Description confirms lower stocks and higher futures.",
    ),
    "7eff3c5f030a6633": entry(
        "positive",
        "More renewables contracting is supportive growth.",
        title_description_rationale="Description confirms contract volume dominance.",
    ),
    "8cd544f4ce1b9f75": entry(
        "negative",
        "Force majeure is an operational disruption.",
        title_description_rationale="Description confirms constrained feedstock supply.",
    ),
    "a1fc2226df4d3118": entry(
        "negative",
        "Buckling carbon market implies weakening conditions.",
        title_description_rationale="Description says carbon prices dropped as bullishness deflated.",
    ),
    "29e21702214e75b4": entry(
        "negative",
        "Force majeure amid supply disruptions is harmful.",
        title_description_rationale="Description confirms the production disruption.",
    ),
    "425428f7d812a690": entry(
        "neutral",
        "Topic-page update title is non-directional.",
        title_description_label="negative",
        title_description_rationale="Description lists more run cuts and supply woes.",
        title_description_ambiguity=True,
    ),
    "325dc99a0ea8111f": entry(
        "positive",
        "Triple-digit LPG price increase is bullish for pricing.",
        title_ambiguity=True,
        title_description_rationale="Description ties the rise to conflict-driven uncertainty.",
    ),
    "4028d1601ba553ba": entry(
        "negative",
        "Cost push and upstream shocks are harmful for polyols buyers.",
        title_description_rationale="Description confirms upward cost pressure from rebate cuts.",
    ),
    "a4629955299e13db": entry(
        "neutral",
        "Outlook hinges on shutdown duration, so title is uncertain.",
        title_ambiguity=True,
        title_description_label="positive",
        title_description_rationale="Description frames the shutdown as supportive for gas markets.",
        title_description_ambiguity=True,
    ),
    "07dc2cf5a2730a6e": entry(
        "neutral",
        "Seeking solutions is factual without a clear market direction.",
        title_description_label="negative",
        title_description_rationale="Description centers on attacks and shipping risk.",
    ),
    "be203ab656250e19": entry(
        "neutral",
        "Trade-route reshaping alone does not show net direction.",
        title_ambiguity=True,
        title_description_label="negative",
        title_description_rationale="Description says exports are costlier and more complex.",
    ),

    # ambiguous
    "5755115cdd367203": entry(
        "neutral",
        "Supply risk and higher prices point in opposite directions.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description says near-term impact is manageable despite soaring spot prices.",
    ),
    "d5ea39ee91b9c8da": entry(
        "neutral",
        "Historic week and premium title is descriptive but mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description notes both correction lower and lingering disruption risk.",
    ),
    "af167c56545f6ae0": entry(
        "negative",
        "Immediate export ban is clearly disruptive.",
        title_description_rationale="Description confirms the abrupt suspension.",
    ),
    "05468055431a1e52": entry(
        "neutral",
        "Skirting shocks but mixed uptake is explicitly mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "f8d5d20c2901f3ac": entry(
        "neutral",
        "Small industrial output rise is macro data only.",
        title_ambiguity=True,
    ),
    "c1562ba97a3a8952": entry(
        "neutral",
        "No oil-stock release plan is policy information only.",
        title_ambiguity=True,
    ),
    "9eed859be19e3483": entry(
        "neutral",
        "Podcast on market-share shifts is analytical, not directional.",
    ),
    "e1c857fa09becc86": entry(
        "neutral",
        "Margin figure alone lacks clear direction.",
        title_ambiguity=True,
        title_description_ambiguity=True,
        title_description_rationale="Description says margins fell month on month but rose year on year.",
    ),
    "d80f2eff3fbfc74e": entry(
        "neutral",
        "Retail fuel price hikes are factual without clear net sentiment.",
        title_ambiguity=True,
    ),
    "4e43ce9f7c5eebd9": entry(
        "neutral",
        "Conditional ETS intervention headline is speculative and mixed.",
        title_ambiguity=True,
        title_description_ambiguity=True,
    ),
    "c6c09748e7b5ae45": entry(
        "positive",
        "No LNG shortages despite disruptions is reassuring.",
        title_ambiguity=True,
        title_description_rationale="Description says stockpiles can cover short-term disruption.",
    ),
    "803b7deeea7d234e": entry(
        "positive",
        "Record oil exports imply stronger trade performance.",
    ),
    "8a336f415debc2b8": entry(
        "negative",
        "Flood-driven export disruption is clearly harmful.",
    ),
    "f2e36e11911aee73": entry(
        "positive",
        "Exceeding 1mn tonnes is a positive export milestone.",
    ),
}
