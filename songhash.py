import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence
from threading import Thread
from queue import Queue

from typer import Typer


APP = Typer()


@dataclass(frozen=True)
class Song:
    path: Path
    contents: bytes
    number: int


def hash_song(song: Song, expected_total: int) -> str:
    print(f"[{song.number}/{expected_total}] {song.path.stem}")
    return hashlib.sha256(song.contents).hexdigest()


def read_song(file: Path, number: int) -> Song:
    with file.open('rb') as f:
        return Song(file, f.read(), number)


def make_hash_dict(songs: Sequence[Path]) -> dict[Path, str]:
    return {path: hash_song(read_song(path, number), expected_total=len(songs)) for number, path in enumerate(songs, start=1)}


def read_all_songs(song_paths: Sequence[Path], q: Queue[Song | None]) -> None:
    for number, file in enumerate(song_paths, start=1):
        q.put(read_song(file, number))

    q.put(None)


def hash_all_songs(q: Queue[Song | None], expected_total: int) -> dict[Path, str]:
    hashes = {}

    while True:
        song = q.get()
        if song is None:
            break
        hashes[song.path] = hash_song(song, expected_total)

    return hashes


def make_hash_dict_threaded(song_paths: Sequence[Path]) -> dict[Path, str]:
    q: Queue[Song | None] = Queue(maxsize=8)

    Thread(target=read_all_songs, args=(song_paths, q)).start()

    return hash_all_songs(q, expected_total=len(song_paths))


@APP.command()
def main(directory: Path, filename: Path) -> None:
    song_paths = [s for s in Path(sys.argv[1]).rglob('*') if s.is_file() and s.suffix in ('.mp3', '.m4a')]

    # hash_dict = make_hash_dict(song_paths)
    hash_dict = make_hash_dict_threaded(song_paths)

    hashes = sorted(list(hash_dict.items()))

    with open(filename, "w") as f:
        for path, song_hash in hashes:
            print(f'{path.relative_to(directory)}: {song_hash}', file=f)


if __name__ == '__main__':
    APP()
