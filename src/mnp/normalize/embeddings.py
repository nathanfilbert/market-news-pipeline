"""Text embeddings for near-duplicate clustering.

Vectors are L2-normalized, so cosine similarity is a dot product. `FastEmbedder` runs a small
local model (ONNX on CPU via fastembed; downloaded once to the cache dir). `HashingEmbedder` is
an offline stand-in for tests and development: hashed word unigrams and bigrams. It captures
wording overlap, not meaning.
"""

import asyncio
import hashlib
import re
from functools import cache
from itertools import pairwise
from pathlib import Path
from typing import Protocol

import numpy as np

DIMS = 384
_WORD = re.compile(r"\w+", re.UNICODE)


class Embedder(Protocol):
    name: str

    def embed(self, texts: list[str]) -> np.ndarray: ...  # (n, dims), rows L2-normalized


def embedding_text(headline: str, summary: str | None) -> str:
    """What gets embedded: the headline plus the start of the summary."""
    return f"{headline}. {(summary or '')[:300]}".strip()


def _normalize(matrix: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return (matrix / np.where(norms == 0, 1, norms)).astype(np.float32)


class FastEmbedder:
    def __init__(self, model: str, cache_dir: Path) -> None:
        from fastembed import TextEmbedding  # imported lazily: loads onnxruntime

        self.name = model
        self._model = TextEmbedding(model, cache_dir=str(cache_dir))

    def embed(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, DIMS), dtype=np.float32)
        return _normalize(np.array(list(self._model.embed(texts)), dtype=np.float32))


class HashingEmbedder:
    name = "hashing"

    def embed(self, texts: list[str]) -> np.ndarray:
        out = np.zeros((len(texts), DIMS), dtype=np.float32)
        for row, text in enumerate(texts):
            words = [w.lower() for w in _WORD.findall(text)]
            for token in words + [f"{a} {b}" for a, b in pairwise(words)]:
                digest = hashlib.blake2b(token.encode(), digest_size=4).digest()
                out[row, int.from_bytes(digest) % DIMS] += 1.0
        return _normalize(out)


@cache
def get_embedder(model: str, cache_dir: Path) -> Embedder:
    return HashingEmbedder() if model == "hashing" else FastEmbedder(model, cache_dir)


async def embed_texts(embedder: Embedder, texts: list[str]) -> np.ndarray:
    """Embed off the event loop (CPU-bound)."""
    return await asyncio.to_thread(embedder.embed, texts)
