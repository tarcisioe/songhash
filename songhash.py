from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, Mapping
from threading import Thread
from queue import Queue

from typer import Typer


APP = Typer()


@dataclass(frozen=True)
class SongFileData:
    path: Path
    timestamp_ns: int

    @staticmethod
    def from_path(p: Path) -> SongFileData:
        return SongFileData(p, p.stat().st_mtime_ns)


@dataclass(frozen=True)
class Song:
    path: Path
    contents: bytes
    number: int
    timestamp_ns: int

    @staticmethod
    def from_file_data(file_data: SongFileData, number: int) -> Song:
        with file_data.path.open('rb') as f:
            return Song(file_data.path, f.read(), number, file_data.timestamp_ns)


@dataclass(frozen=True)
class SongHash:
    path: Path
    sha256: str
    timestamp_ns: int

    @staticmethod
    def from_song(song: Song) -> SongHash:
        return SongHash(
            song.path,
            hashlib.sha256(song.contents).hexdigest(),
            song.timestamp_ns,
        )


Database = Mapping[Path, SongHash]


def read_songs_into_queue(song_file_data: Sequence[SongFileData], q: Queue[Song | None]) -> None:
    for number, file_data in enumerate(song_file_data, start=1):
        q.put(Song.from_file_data(file_data, number))

    q.put(None)


def hash_songs_from_queue(q: Queue[Song | None], expected_total: int) -> list[SongHash]:
    hashes = []

    while True:
        song = q.get()
        if song is None:
            break
        print(f"[{song.number}/{expected_total}] {song.path}")
        hashes.append(SongHash.from_song(song))

    return hashes


def hash_songs(song_file_data: Sequence[SongFileData]) -> list[SongHash]:
    q: Queue[Song | None] = Queue(maxsize=8)

    Thread(target=read_songs_into_queue, args=(song_file_data, q)).start()

    return hash_songs_from_queue(q, expected_total=len(song_file_data))


def get_old_database(filename: Path) -> Database | None:
    if not filename.exists():
        return None

    print("Previous data exists. Updating...")


    with filename.open("r") as f:
        split_lines = (line.split('\t') for line in f.readlines())

        return {
            (path := Path(file)): SongHash(path, sha256, int(timestamp_ns))
            for file, sha256, timestamp_ns in
            split_lines
        }


def get_timestamp_from_database(database: Database, song_path: Path) -> int:
    song_hash = database.get(song_path)

    if song_hash is None:
        return 0

    return song_hash.timestamp_ns


def get_songs_to_update(songs_pre_check: Sequence[SongFileData], old_database: Database | None) -> list[SongFileData]:
    if old_database is None:
        return list(songs_pre_check)

    return [
        s
        for s in songs_pre_check
        if s.timestamp_ns > get_timestamp_from_database(old_database, s.path)
    ]


def merge(old_database: Database | None, new_hashes: Sequence[SongHash]) -> list[SongHash]:
    if old_database is None:
        return list(new_hashes)

    database = dict(old_database)
    database.update((s.path, s) for s in new_hashes)
    return list(database.values())


def output(hashes: Sequence[SongHash], filename: Path) -> None:
    hashes = sorted(hashes, key=lambda h: h.path)

    with open(filename, "w") as f:
        for song_hash in hashes:
            print(f'{song_hash.path}\t{song_hash.sha256}\t{song_hash.timestamp_ns}', file=f)


@APP.command()
def main(directory: Path, filename: Path) -> None:
    song_paths = [SongFileData(s, s.stat().st_mtime_ns) for s in Path(sys.argv[1]).rglob('*') if s.is_file() and s.suffix in ('.mp3', '.m4a')]
    old_database = get_old_database(filename)

    songs_to_update = get_songs_to_update(song_paths, old_database)

    if not songs_to_update:
        print("No new songs to update. Done!")
        return

    new_hashes = hash_songs(songs_to_update)

    output(merge(old_database, new_hashes), filename)


if __name__ == '__main__':
    APP()
