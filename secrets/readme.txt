Files in here are referenced by notebooks and tests but not in src.

These are usually .json files corresponding to keyword args used by a client

Example usage:

elasticsearch.json
```
{
    "cloud_id":<CLOUD_ID_VALUE>,
    "api_key":<API_KEY_VALUE>
}
```

python
```
from elasticsearch.client import Elasticsearch
from pathlib import Path
import json

config = Path('secrets', 'elasticsearch.json')
elasticsearch = Elasticsearch(
    **json.loads(config.read_text())
)
```
