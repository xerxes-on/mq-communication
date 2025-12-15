# Contributing

Contributions are welcome and will be fully credited.

## Pull Requests

- **[PSR-12 Coding Standard](https://github.com/php-fig/fig-standards/blob/master/accepted/PSR-12-extended-coding-style-guide.md)** - Run `composer format` to apply fixes.
- **Add tests** - Your patch won't be accepted if it doesn't have tests.
- **Document any change in behaviour** - Make sure the README and any other relevant documentation are kept up-to-date.
- **One pull request per feature** - If you want to do more than one thing, send multiple pull requests.

## Running Tests

```bash
composer test
```

## Code Style

Fix code style with:

```bash
composer format
```

Or check style without fixing:

```bash
composer format:check
```

## Static Analysis

Run PHPStan:

```bash
composer analyse
```
