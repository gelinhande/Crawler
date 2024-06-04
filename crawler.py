
import logging
import re
from urllib.parse import urlparse
from collections import defaultdict, Counter
from lxml import html
from bs4 import BeautifulSoup

import requests

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.maxValidOutLinks = ["", float("-inf")]
        self.numberOfLinksCrawled = 0
        self.TRAP_PARAMS = {"action=download", "action=login", "action=edit"}
        self.domainCounts = defaultdict(int)
        self.visited_urls = set()
        self.domainAndOutputLinks = defaultdict(int)
        self.trapUrls = []
        self.longest_page_length = 0
        self.common_words_counter = Counter()

        self.STOP_WORDS = {'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are',
                           "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both',
                           'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does',
                           "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further',
                           'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's",
                           'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i',
                           "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its',
                           'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of',
                           'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out',
                           'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't",
                           'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them',
                           'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're",
                           "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was',
                           "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's",
                           'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why',
                           "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've",
                           'your', 'yours', 'yourself', 'yourselves'}

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)
            self.track_visited_urls(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
        self.write_results_to_file()

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []
        if url_data['content'] is not None and url_data['http_code'] != 404:
            try:
                document = html.fromstring(url_data['content'])
                # Final url should not be further redirected and redirection should be reflected in ‘url_data’ dict in
                # fetch_url method
                base_url = url_data['final_url'] if url_data['is_redirected'] else url_data['url']
                anchors = document.xpath('//a')
                for anchor in anchors:
                    href = anchor.get('href')
                    if href:
                        # Relative links should be converted to absolute forms
                        absolute_url = requests.compat.urljoin(base_url, href)
                        outputLinks.append(absolute_url)
                        self.domainAndOutputLinks[base_url] += 1
                        # Track the longest page
                page_length = len(document.text_content().split())
                if page_length > self.longest_page_length:
                    self.longest_page_length = page_length

                # Track common words
                self.track_common_words(document.text_content())
            except Exception as e:
                # Handle exceptions
                print("failed")
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        #         print(parsed.netloc)
        if self.is_trap(parsed):
            self.trapUrls.append(parsed.netloc + parsed.path)
            return False

        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$",
                                    parsed.path.lower())  # and urllib.request.urlopen(url).getcode() == 200

        except TypeError:
            print("TypeError for ", parsed)
            return False

    def is_trap(self, parsed):
        # if parsed[:6] == "mailto":
        #     return True
        # Return false if we receive some parameters that know are bad
        if parsed.query in self.TRAP_PARAMS:
            return True

        # History traps detection implemented
        # If the URL has been accessed more than 13 times return false
        domainName = parsed.netloc + parsed.path
        self.domainCounts[domainName] += 1
        if self.domainCounts[domainName] > 13:
            return True

        # History traps detection implemented
        if domainName not in self.visited_urls:
            self.visited_urls.add(domainName)
            # Proceed with crawling tasks for the URL
            return True

        if parsed.scheme not in set(["http", "https"]):
            return True
        # Checking long urls implemented
        if len(parsed.geturl()) > 200:
            return True

        # continuously repeating sub-directories
        if re.search(r'(/.+?/)\1', parsed.path):
            return True

        return False

    def update_subdomains_count(self, url):
        parsed = urlparse(url)
        subdomain = parsed.hostname
        self.domainCounts[subdomain] += 1

    def track_visited_urls(self, url):
        parsed = urlparse(url)
        domain_path = parsed.netloc + parsed.path
        if self.is_trap(parsed):
            self.trapUrls.append(domain_path)
        self.visited_urls.add(domain_path)

    def track_common_words(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text()
        words = re.findall(r'\b[a-zA-Z]{2,}\b', text.lower())  # Adjusted regex to match words with 2+ letters

        # Exclude stop words and now also single-letter words
        filtered_words = [word for word in words if word not in self.STOP_WORDS]

        # Update common words counter
        self.common_words_counter.update(filtered_words)

    def write_results_to_file(self):
        output_file_name = 'crawler_results.txt'
        with open(output_file_name, 'w') as file:
            file.write(f"Number of unique pages encountered: {len(self.visited_urls)}\n\n")

            # Page with the most valid out links
            file.write("Page with the most valid out links:\n")
            max_out_links_url, max_out_links_count = max(self.domainAndOutputLinks.items(), key=lambda item: item[1])
            file.write(f"{max_out_links_url}: {max_out_links_count} out links\n\n")

            # Subdomains visited and URL counts
            file.write("Subdomains visited and their URL counts:\n")
            longest_page_length = -1;
            longest_page_url = None;
            for subdomain, count in sorted(self.domainCounts.items(), key=lambda item: item[1], reverse=True):
                file.write(f"{subdomain}: {count}\n")
                if count > longest_page_length:
                    longest_page_length = count
                    longest_page_url = subdomain
            file.write("\n")

            # Longest page in terms of number of words
            file.write("Longest page in terms of number of words:\n")
            file.write(f"URL: {longest_page_url} with {longest_page_length} words\n\n")

            # 50 most common words
            file.write("50 most common words (excluding stop words) and their frequency:\n")
            for word, count in self.common_words_counter.most_common(50):
                file.write(f"{word}: {count}\n")
            file.write("\n\n")

            # List of downloaded URLs and identified traps
            file.write("List of downloaded URLs:\n")
            for url in self.visited_urls:
                file.write(f"{url}\n")
            file.write("\nIdentified traps:\n")
            for trap in self.trapUrls:
                file.write(f"{trap}\n")
            file.write("\n")

        print(f"Results written to {output_file_name}")
