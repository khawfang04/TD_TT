import yaml
import re
import os
from collections import Counter
import datetime

# ct stores current time
ct = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

class InputReader:
    def read(self, source):
        raise NotImplementedError("This method should be overridden by subclasses.")


class FileInputReader(InputReader):
    def read(self, input_path,source_file):
        with open(f'{input_path}/{source_file}', 'r', encoding='utf-8') as file:
            return file.read()


class OutputWriter:
    def write(self, target, sanitized_text, statistics):
        raise NotImplementedError("This method should be overridden by subclasses.")


class ConsoleOutputWriter(OutputWriter):
    def write(self, target, sanitized_text, statistics):
        print("Sanitized Text:")
        print(sanitized_text)
        print("\nStatistics:")
        for stat_name, stat_value in statistics.items():
            print(f"{stat_name}: {stat_value}")


class DirectoryOutputWriter(OutputWriter):
    def write(self, target, sanitized_text, statistics):
        os.makedirs(target, exist_ok=True)

        # Write sanitized text to a file
        sanitized_file = os.path.join(target, f"sanitized_text_{ct}.txt")
        with open(sanitized_file, 'w', encoding='utf-8') as file:
            file.write(sanitized_text)

        # Write statistics to a file
        statistics_file = os.path.join(target, f"statistics_{ct}.txt")
        with open(statistics_file, 'w', encoding='utf-8') as file:
            for stat_name, stat_value in statistics.items():
                if isinstance(stat_value, dict):
                    for char, count in stat_value.items():
                        file.write(f"{char}: {count}\n")
                else:
                    file.write(f"{stat_name}: {stat_value}\n")


class TextSanitizer:
    def __init__(self):
        self.steps = []

    def add_step(self, step):
        self.steps.append(step)

    def sanitize(self, text):
        for step in self.steps:
            text = step(text)
        return text


class StatisticsGenerator:
    def __init__(self, stat_types):
        self.stat_types = stat_types

    def generate(self, text):
        statistics = {}
        if "alphabet_count" in self.stat_types:
            statistics["alphabet_count"] = self._alphabet_count(text)
        if "most_used_alphabet" in self.stat_types:
            statistics["most_used_alphabet"] = self._most_used_alphabet(text)
        return statistics

    def _alphabet_count(self, text):
        counts = Counter(char for char in text if char.isalpha())
        return dict(sorted(counts.items()))

    def _most_used_alphabet(self, text):
        counts = Counter(char for char in text if char.isalpha())
        if counts:
            most_common = counts.most_common(1)[0]
            return f"{most_common[0]} ({most_common[1]})"
        return "None"


# Sanitization steps
def lowercase(text):
    return text.lower()


def replace_tabs(text):
    return text.replace("\t", "____")


def normalize_whitespace(text):
    return re.sub(r'\s+', ' ', text).strip()


def remove_non_ascii(text):
    return ''.join(char for char in text if char.isascii())

def convert_email(text):
    """
    Replace parts of email addresses (before the '@') with a placeholder 'hidden_email'.
    The domain must not contain special characters, and the username must not have spaces.
    """
    return re.sub(r'\S+@\w+\.\w+', 'email@domain.com', text)



class TextSanitizerApp:
    def __init__(self, input_reader, output_writer, sanitizer, stats_generator,source_file):
        self.input_reader = input_reader
        self.output_writer = output_writer
        self.sanitizer = sanitizer
        self.stats_generator = stats_generator
        self.source_file = source_file

    def run(self, input_path, target,source_file):
        input_text = self.input_reader.read(input_path,source_file)
        sanitized_text = self.sanitizer.sanitize(input_text)
        statistics = self.stats_generator.generate(sanitized_text)
        self.output_writer.write(target, sanitized_text, statistics)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Text Sanitizer Application")
    parser.add_argument("--config", required=True, help="Path to the YAML configuration file")
    args = parser.parse_args()

    # Load configuration from YAML file
    with open(args.config, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    source_file = config["source"]["source_file"]
    input_path = config["source"]["input_path"]
    output_path = config["source"]["output_path"]
    transformations = config["transform"]
    stat_types = config["stat_type"]

    # Setup components
    input_reader = FileInputReader()
    if output_path == "console":
        output_writer = ConsoleOutputWriter()
    else:
        output_writer = DirectoryOutputWriter()

    sanitizer = TextSanitizer()
    if "lowercase" in transformations:
        sanitizer.add_step(lowercase)
    if "whitespace" in transformations:
        sanitizer.add_step(normalize_whitespace)
    if "tab" in transformations:
        sanitizer.add_step(replace_tabs)
    if "ascii" in transformations:
        sanitizer.add_step(remove_non_ascii)
    if "email" in transformations:
        sanitizer.add_step(convert_email)

    stats_generator = StatisticsGenerator(stat_types)

    # Run the application
    app = TextSanitizerApp(input_reader, output_writer, sanitizer, stats_generator,source_file)
    app.run(input_path, output_path,source_file)
    print('done process')
