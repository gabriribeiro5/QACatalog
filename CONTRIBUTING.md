## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

#### >> Commiting it right

All commits must comply with [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/). See how:

>USE COMMITZEN

DON'T use ```git commit```!

Instead, use **commitizen** to help you create usefull messages and **prevent git commit hook to run and reject your commit**.

With your **local environment setup and activated**, run the following command:
```
cz commit
```

Select the type of change you are committing (Use arrow keys), Then answer the given questions regarding your change (press enter after each answer).

[note]: You can check your commits by running ```git log``` on your terminal.


>WHY COMMITZEN

Our commits contain the following structural elements, to communicate intent to the consumers of this library:

- fix: a commit of the type fix patches a bug in your codebase (this correlates with PATCH in Semantic Versioning).
- feat: a commit of the type feat introduces a new feature to the codebase (this correlates with MINOR in Semantic Versioning).
- BREAKING CHANGE: a commit that has a footer BREAKING CHANGE:, or appends a ! after the type/scope, introduces a breaking API change (correlating with MAJOR in Semantic Versioning). A BREAKING CHANGE can be part of commits of any type.
- types other than 'fix:' and 'feat:' are allowed, for example 'build:', 'chore:', 'ci:', 'docs:', 'style:', 'refactor:', 'perf:', 'test:', and others.


See [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) for more information.

## Dev's overview:

#### >> Archtecture: application's design

>ENTITIES:

Brief info about the app's entities.

#### >> Engineering: build & CI/CD

>MAIN TOOLS:

**Virtual environments**:

- .venv_global: an optimized environment containing all (and only) necessary libs for the package. 

**When testing** new features or debugging existing ones, this environment **must be cloned into a local folder**, so you can freely install (or uninstall) source packages.

**Before commiting and pushing**, make sure to **rerun the application using the global environment** (.venv_global) so the missing packages for your new features are required and you can propperly install them.

- .venv_local: this one is meant to be used only in your local machine and must not be pushed into master nor release-candidates.

>VERSION CONTROL:

Our versions are setup as MAJOR.MINOR.PATCH (1.0.0):

1. MAJOR: when changes are incompatible with the previous version;
2. MINOR: when added new functionalities keeping compatibility;
3. PATCH: adjusting flaws and keeping compatibility.

See [Versioning] (https://semver.org/lang/pt-BR/) for more information.

[Commits] (https://www.conventionalcommits.org/en/v1.0.0/)

## License
[MIT](https://choosealicense.com/licenses/mit/)