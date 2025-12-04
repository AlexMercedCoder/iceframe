#!/usr/bin/env python3
"""
Script to combine all markdown files from a subdirectory into a single file.

Usage:
    python combine_markdown.py <subdirectory> <output_filename>
    
Example:
    python combine_markdown.py docs combined_docs
    
This will create combined_docs.md with all markdown files from the docs/ directory.
"""

import argparse
import os
from pathlib import Path
import sys


def find_markdown_files(directory: str) -> list[Path]:
    """
    Recursively find all markdown files in the given directory.
    
    Args:
        directory: Path to the directory to search
        
    Returns:
        List of Path objects for all .md files found
    """
    directory_path = Path(directory)
    
    if not directory_path.exists():
        raise FileNotFoundError(f"Directory '{directory}' does not exist")
    
    if not directory_path.is_dir():
        raise NotADirectoryError(f"'{directory}' is not a directory")
    
    # Find all .md files recursively
    markdown_files = sorted(directory_path.rglob("*.md"))
    
    return markdown_files


def combine_markdown_files(markdown_files: list[Path], output_file: str) -> None:
    """
    Combine all markdown files into a single output file.
    
    Args:
        markdown_files: List of Path objects for markdown files to combine
        output_file: Name of the output file (without .md extension)
    """
    # Ensure output filename has .md extension
    if not output_file.endswith('.md'):
        output_file = f"{output_file}.md"
    
    output_path = Path(output_file)
    
    with open(output_path, 'w', encoding='utf-8') as outfile:
        for i, md_file in enumerate(markdown_files):
            # Add separator between files (except before the first file)
            if i > 0:
                outfile.write("\n\n---\n\n")
            
            # Write a header indicating the source file
            outfile.write(f"<!-- Source: {md_file} -->\n\n")
            
            # Read and write the content of the markdown file
            try:
                with open(md_file, 'r', encoding='utf-8') as infile:
                    content = infile.read()
                    outfile.write(content)
            except Exception as e:
                print(f"Warning: Could not read {md_file}: {e}", file=sys.stderr)
                continue
    
    print(f"Successfully combined {len(markdown_files)} markdown files into {output_path}")


def main():
    """Main function to parse arguments and execute the script."""
    parser = argparse.ArgumentParser(
        description="Combine all markdown files from a subdirectory into a single file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s docs combined_docs
  %(prog)s ./documentation all_docs.md
        """
    )
    
    parser.add_argument(
        'subdirectory',
        help='Path to the subdirectory containing markdown files'
    )
    
    parser.add_argument(
        'output_filename',
        help='Name for the output file (with or without .md extension)'
    )
    
    args = parser.parse_args()
    
    try:
        # Find all markdown files
        markdown_files = find_markdown_files(args.subdirectory)
        
        if not markdown_files:
            print(f"No markdown files found in '{args.subdirectory}'", file=sys.stderr)
            sys.exit(1)
        
        print(f"Found {len(markdown_files)} markdown files in '{args.subdirectory}'")
        
        # Combine them into a single file
        combine_markdown_files(markdown_files, args.output_filename)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
