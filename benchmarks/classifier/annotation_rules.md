# Annotation Rules

This benchmark scores `classifier.py` against a manually reviewed stratified
sample from `data/feed.json`.

General rules:

- Score categories as article-level sets drawn only from the canonical contract:
  `Oil - Crude`, `Oil - Refined Products`, `Natural Gas`, `LNG`, `Coal`,
  `Electric Power`, `Energy Transition`, `Chemicals`, `Metals`,
  `Agriculture`, `Fertilizers`, `Shipping`, `General`.
- `General` is valid only when the available title or title+description snippet
  is genuinely non-specific, cross-commodity without a dominant commodity, or
  mostly policy/geopolitics with no clear commodity anchor.
- Do not award `General` just because the headline is broad. If the text clearly
  centers on one commodity family, use that family.
- Use at most two categories. Dual labels are justified only when both
  commodity families are explicit and operationally central, not merely causal
  background.
- Choose the primary category as the article's main tradable subject. If one
  commodity is the direct market or plant subject and another is context, the
  direct subject is primary.
- Title-only gold uses only the headline text. Title+description gold may
  upgrade, disambiguate, or add a second category if the description snippet
  makes it explicit.

Commodity-family rules:

- `Natural Gas` vs `LNG`:
  Use `LNG` for liquefied natural gas cargoes, LNG plants, JKM, regas/LNG trade,
  and LNG-specific shipping. Use `Natural Gas` for pipelines, hubs, storage,
  power-sector gas burn, and generic gas supply unless LNG is explicit.
- `Oil - Crude` vs `Oil - Refined Products`:
  Use `Oil - Crude` for crude grades, upstream supply, OPEC, sanctions on crude,
  and crude price direction. Use `Oil - Refined Products` for refineries, diesel,
  gasoline, jet fuel, naphtha, fuel oil, LPG, and refining margins. Dual-label
  only when both crude and refinery/products are explicit first-order topics.
- `Chemicals` vs `Fertilizers`:
  Use `Fertilizers` for fertilizer trade, ammonia/urea/phosphates/potash when
  the article is clearly about fertilizer markets or producers. Use
  `Chemicals` for petrochemical chains, polymers, solvents, aromatics, olefins,
  methanol, and industrial chemicals. Ammonia can fall under `Energy Transition`
  or `Chemicals` only when fertilizer context is absent.
- `Agriculture` vs `Fertilizers`:
  Use `Agriculture` for grains, oilseeds, sugar, proteins/feed, and farm-crop
  flows. Use `Fertilizers` for nutrient inputs even if crops are mentioned.
- `Energy Transition` vs conventional energy:
  Use `Energy Transition` for renewables, carbon markets, biofuels, hydrogen,
  low-carbon certificates, decarbonization, and transition-policy topics. Do
  not use it for conventional oil, gas, coal, or power stories unless the
  transition commodity itself is a main subject.
- `Shipping`:
  Use `Shipping` when freight rates, vessel movements, tanker availability,
  bunker markets, or maritime disruptions are core to the story. A shipping
  side-effect alone does not justify a dual label.

Process/company/policy rules:

- Plant, outage, force majeure, or company headlines belong to the commodity
  produced, consumed, or traded by that asset when the commodity is explicit.
- Broad policy, war, or macro headlines with no clear commodity anchor may stay
  `General`.
- "Energy" or "market" headlines should be classified to a specific commodity
  only if the available text makes that commodity dominant.

Ambiguity policy:

- Set `ambiguity=True` when two reasonable readings remain after applying the
  rules, or when title-only evidence is materially weaker than title+description.
- Keep the rationale short and concrete. It should explain why the chosen
  category or dual label is the best fit for the available text.
