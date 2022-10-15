
from dataclasses import dataclass


@dataclass
class Word:
    word: str
    location: int
    id_: int = 0

@dataclass
class Link:
    link: str
    id_: int = 0
