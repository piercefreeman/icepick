# iceaxe

A modern, fast ORM for Python. We have the following goals:

- **Performance**: We want to exceed or match the fastest ORMs in Python. We want our ORM to be as close as possible to raw-[asyncpg](https://github.com/MagicStack/asyncpg) speeds.
- **Typehinting**: Everything should be typehinted with expected types.
- **Postgres only**: Leverage native Postgres features and simplify the implementation.
- **Common is easy, rare is possible**: If you're writing _really_ complex queries, these are better done by hand so you can see exactly what SQL will be run by.

Iceaxe is an independent project. It's compatible with the [Mountaineer](https://github.com/piercefreeman/mountaineer) ecosystem, but you can use it in whatever
project and web framework you're using.
