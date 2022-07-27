# Certified Assistant
**A questions reminder for certified professionals, based on mock exams**

If you are a developer and wish to **contribute** with this app's evolution: Please, have a look at our **CONTRIBUTING.md** file we made specially for you and note that we already have a space for you in our heart.

> SUMMARY:
- Intro
- Installation
- Usage
- Contributing
- Dev overview: Archtecture, Build & CI/CD
- License

## Intro

This application supports a wide range of adjustments. Allowing you, not only to reset your input parameters, but also to rethink the output structure by adjusting its config file. Upcomming versions may allow us to leverage from text data, making it usefull for ETL, Data Analytics and even test some Machine Learning models and hypothesis.

>MAIN FUNCTIONS:

1. [data extraction]: Search for questions in PDF files
2. [data preparation]: Catalog all questions in YAML files with deeper information such as page number, keywords and subject of the content of the
3. [user interaction]: Get instructions (keywords and optional configurations) from User Interface 
4. [search engine - primary service]: Send user instructions to the search engine
5. [user interaction]: Returns all related questions and its informations to the User Interface
6. [async user interaction and data extraction]: Awaits for further instructions while searching for new PDF files

#### >> glossary

The current version of this aplication does not demand any special term explanation. The following topic is just a sample tesxt for future descriptions.

>SAMPLE TERM:

Here is a description of ***sample term***:
- This is a detail of sample term
- This is another detail of it

## Installation

There are no installation instructions yet.

> installation text exemple:

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install exemple
```

## Usage

Adjustable parameters may be found at configfiles directory.
For further understanding of the application, start by checking out the input and output files at configfiles.

> usage text exemple:
```python
import foobar

# returns 'words'
foobar.pluralize('word')

# returns 'geese'
foobar.pluralize('goose')

# returns 'phenomenon'
foobar.singularize('phenomena')
```

## License
[MIT](https://choosealicense.com/licenses/mit/)