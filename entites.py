
from dataclasses import dataclass


@dataclass
class Word:
    word: str
    location: int
    id_: int = 0

@dataclass
class Link:
    link: str
    text: str = ""
    id_: int = 0

@dataclass
class Element:
    word: str
    location: int = 0
    word_id: int = 0
    href: str = ""
    link_id: int = 0

@dataclass
class LinkToGo:
    link: str
    depth: int = 0

    def __hash__(self) -> int:
        return hash(self.link)
