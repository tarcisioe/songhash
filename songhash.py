from __future__ import annotations

import hashlib
import sys
from enum import Enum
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Sequence, Mapping
from threading import Thread
from queue import Queue

import typer


APP = typer.Typer()


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


@dataclass(frozen=True, order=True)
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


@dataclass
class Database:
    base_directory: Path
    hashes: dict[Path, SongHash]

    def is_newer_than_recorded(self, song_file_data: SongFileData) -> int:
        song_hash = self.hashes.get(song_file_data.path.relative_to(self.base_directory))

        if song_hash is None:
            return True

        return song_file_data.timestamp_ns > song_hash.timestamp_ns

    def matches_recorded_hash(self, song_hash: SongHash) -> bool:
        return self.hashes[song_hash.path].sha256 == song_hash.sha256

    def update(self, new_hashes: Sequence[SongHash]) -> None:
        self.hashes.update(
            ((path := s.path.relative_to(self.base_directory)), replace(s, path=path))
            for s in new_hashes
        )


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


def read_database(filename: Path) -> Database | None:
    if not filename.exists():
        return None

    with filename.open("r") as f:
        header, *file_lines = (l.strip() for l in f.readlines())
        split_lines = (line.split('\t') for line in file_lines)

        base_directory = Path(header)

        return Database(
            base_directory,
            {
                (path := Path(file)): SongHash(path, sha256, int(timestamp_ns))
                for file, sha256, timestamp_ns in
                split_lines
            }
        )


def get_timestamp_from_database(database: Database, song_path: Path) -> int:
    song_hash = database.hashes.get(song_path.relative_to(database.base_directory))

    if song_hash is None:
        return 0

    return song_hash.timestamp_ns


def get_songs_to_update(songs_pre_check: Sequence[SongFileData], old_database: Database) -> list[SongFileData]:
    return [
        s
        for s in songs_pre_check
        if old_database.is_newer_than_recorded(s)
    ]


def output(database: Database, filename: Path) -> None:
    hashes = sorted(database.hashes.values())

    with open(filename, "w") as f:
        print(database.base_directory, file=f)
        for song_hash in hashes:
            print(f'{song_hash.path}\t{song_hash.sha256}\t{song_hash.timestamp_ns}', file=f)


@dataclass
class Diff:
    added: list[Path]
    removed: list[Path]
    modified: list[Path]


def diff_databases(older: Database, newer: Database) -> Diff:
    added: list[Path] = []
    removed: list[Path] = []
    modified: list[Path] = []

    for path, song_hash in older.hashes.items():
        if path not in newer.hashes:
            removed.append(path)
            continue

        if not newer.matches_recorded_hash(song_hash):
            modified.append(path)

    for path in newer.hashes:
        if path not in older.hashes:
            added.append(path)

    return Diff(added, removed, modified)


def ensure_database(base_directory: Path, filename: Path) -> Database:
    database = read_database(filename)

    if database is None:
        return Database(base_directory, {})

    if database.base_directory != base_directory:
        print("Cowardly refusing to update a database for a different directory.", file=sys.stderr)
        raise typer.Exit(1)

    print("Previous data exists. Updating...")
    return database


@APP.command()
def scan(directory: Path, filename: Path, base_directory: Path | None = None) -> None:
    base_directory = directory if base_directory is None else base_directory

    base_directory = base_directory.absolute()
    directory = directory.absolute()

    database = ensure_database(base_directory, filename)

    song_paths = [SongFileData(s, s.stat().st_mtime_ns) for s in directory.rglob('*') if s.is_file() and s.suffix in ('.mp3', '.m4a')]
    songs_to_update = get_songs_to_update(song_paths, database)

    if not songs_to_update:
        print("No new songs to update. Done!")
        return

    new_hashes = hash_songs(songs_to_update)

    database.update(new_hashes)

    output(database, filename)


@APP.command()
def diff(older_path: Path, newer_path: Path) -> None:
    older = read_database(older_path)

    if older is None:
        raise typer.Exit(1)

    newer = read_database(newer_path)

    if newer is None:
        raise typer.Exit(1)

    diff = diff_databases(older, newer)

    print("Added:")
    for path in diff.added:
        print(path)
    print()
    print("Removed:")
    for path in diff.removed:
        print(path)
    print()
    print("Modified:")
    for path in diff.modified:
        print(path)


if __name__ == '__main__':
    APP()
