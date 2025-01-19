import sys
import os
import string
from collections import Counter
import argparse

#     if len(sys.argv) == 3:
class InputReader:
    """Base class for reading input data."""
    def read(self, source):
        raise NotImplementedError("This method should be overridden by subclasses.")

class FileInputReader(InputReader):
    """Reads input from a file."""
    def read(self, source):
        if not os.path.exists(source):
            raise FileNotFoundError(f"Source file '{source}' not found.")
        with open(source, 'r', encoding='utf-8') as file:
            return file.read()

class OutputWriter:
    """Base class for writing output data."""
    def write(self, target, sanitized_text, statistics):
        raise NotImplementedError("This method should be overridden by subclasses.")

class ConsoleOutputWriter(OutputWriter):
    """Writes output to the console."""
    def write(self, target, sanitized_text, statistics):
        print("Sanitized Text:")
        print(sanitized_text)
        print("\nStatistics:")
        for char, count in statistics.items():
            print(f"{char}: {count}")

class TextSanitizer:
    """Sanitizes the text using various operations."""
    def __init__(self):
        self.steps = []

    def add_step(self, step):
        """Adds a sanitization step."""
        self.steps.append(step)

    def sanitize(self, text):
        """Applies all sanitization steps to the text."""
        for step in self.steps:
            text = step(text)
        return text

class StatisticsGenerator:
    """Generates statistics from sanitized text."""
    def generate(self, text):
        """Generates a count of each alphabet and orders it alphabetically."""
        counts = Counter(char for char in text if char.isalpha())
        return dict(sorted(counts.items()))  # Sort by alphabet


class TextSanitizerApp:
    """Main application class for the text sanitizer."""
    def __init__(self, input_reader, output_writer, sanitizer, stats_generator):
        self.input_reader = input_reader
        self.output_writer = output_writer
        self.sanitizer = sanitizer
        self.stats_generator = stats_generator

    def run(self, source, target):
        # Read input
        input_text = self.input_reader.read(source)

        # Sanitize text
        sanitized_text = self.sanitizer.sanitize(input_text)

        # Generate statistics
        statistics = self.stats_generator.generate(sanitized_text)

        # Write output
        self.output_writer.write(target, sanitized_text, statistics)

# Default sanitization steps
def lowercase(text):
    return text.lower()

def replace_tabs(text):
    return text.replace("\t", "____")

# CLI argument handling and app execution
if __name__ == "__main__":
    
    # CLI argument parsing
    parser = argparse.ArgumentParser(description="Text Sanitizer Application")
    parser.add_argument("--source", required=True, help="Path to the input text file")
    parser.add_argument("--target", default="console", help="Output target (e.g., 'console')")
    args = parser.parse_args()

    # Setup components
    input_reader = FileInputReader()
    output_writer = ConsoleOutputWriter()
    sanitizer = TextSanitizer()
    sanitizer.add_step(lowercase)
    sanitizer.add_step(replace_tabs)
    stats_generator = StatisticsGenerator()

    # Run the application
    app = TextSanitizerApp(input_reader, output_writer, sanitizer, stats_generator)
    app.run(args.source, args.target)
