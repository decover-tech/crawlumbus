# Crawlumbus

Web Crawler

## Description

This project provides a web crawler implementation using Scrapy library. It allows crawling a specified website and extracting its content.

## Installation

1. Clone the repository:

```shell
git clone https://github.com/your-username/web-crawler.git
```

2. Change into the project directory:

```shell
cd web-crawler
```

3. Install the required dependencies:

```shell
pip install -r requirements.txt
```

## Usage

1. Open the `main.py` file in your preferred text editor.

2. Set the desired website URL and domain in the `website` and `domain` variables:

```python
website = 'https://www.example.com/'
domain = 'example.com'
```

3. Run the script:

```shell
python main.py
```

## Output

The script will crawl the specified website and print the following information for each page:

- URL: The URL of the page.
- Content: The content extracted from the page.

## Contributing

Contributions are welcome! If you find any issues or want to add new features, please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
