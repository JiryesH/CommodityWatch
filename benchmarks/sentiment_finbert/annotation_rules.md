# Sentiment Annotation Rules

This benchmark scores `sentiment_finbert.py` against a manually reviewed sample
from `data/feed.json`.

Core rule:

- Label sentiment from the market or news interpretation of the headline, not
  from emotional wording alone.

Label definitions:

- `positive`: supportive, bullish, tightening, higher-margin, higher-price, or
  otherwise improving conditions for the market subject in context.
- `negative`: harmful, bearish, weakening, oversupplied, lower-margin, or
  otherwise disruptive conditions for the market subject in context.
- `neutral`: mostly factual, mixed, unclear, or not directionally meaningful.

Mode rules:

- `title` gold uses only the title text. If the title is directionally unclear
  without the description, prefer `neutral` and mark `ambiguity=true`.
- `title_description` gold uses title plus the available description snippet.
  If the description resolves the market direction, label it accordingly.

Commodity-domain interpretation rules:

- Higher prices are not automatically positive or negative in every context.
  Judge whether the headline implies tighter supply, stronger demand, better
  producer economics, weaker demand, oversupply, or downstream cost pain.
- Supply disruptions, sanctions, outages, export curbs, and conflict are often
  bullish for the affected commodity market but can stay `neutral` when the
  impact is unclear or mixed.
- Headlines about policy, taxes, quotas, and macro data stay `neutral` unless
  the directional market implication is explicit in the text available for that
  mode.
- Mixed headlines with offsetting cues should usually remain `neutral`.

Ambiguity policy:

- `ambiguity=true` means a reasonable reviewer could defend more than one label
  from the text in that mode.
- Ambiguous rows are still scored; the flag is for audit transparency and error
  analysis.
