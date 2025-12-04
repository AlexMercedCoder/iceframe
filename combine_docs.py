#!/usr/bin/env python3
"""
Script to combine all markdown files from the docs folder into one big markdown file.
The output file (combined_docs.md) is automatically added to .gitignore.
"""

import os
from pathlib import Path
from datetime import datetime


def combine_markdown_files(docs_dir: str = "docs", output_file: str = "combined_docs.md"):
    """
    Combine all markdown files from docs directory into a single file.
    
    Args:
        docs_dir: Path to the docs directory (default: "docs")
        output_file: Name of the output file (default: "combined_docs.md")
    """
    # Get the project root directory (where this script is located)
    project_root = Path(__file__).parent
    docs_path = project_root / docs_dir
    output_path = project_root / output_file
    
    if not docs_path.exists():
        print(f"Error: Docs directory '{docs_path}' not found!")
        return
    
    # Find all markdown files recursively
    md_files = sorted(docs_path.rglob("*.md"))
    
    if not md_files:
        print(f"No markdown files found in '{docs_path}'")
        return
    
    print(f"Found {len(md_files)} markdown files")
    
    # Create the combined markdown file
    with open(output_path, 'w', encoding='utf-8') as outfile:
        # Write header
        outfile.write(f"# Combined Documentation\n\n")
        outfile.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        outfile.write(f"This file combines all markdown documentation from the `{docs_dir}` folder.\n\n")
        outfile.write("---\n\n")
        
        # Write table of contents
        outfile.write("## Table of Contents\n\n")
        for md_file in md_files:
            relative_path = md_file.relative_to(docs_path)
            # Create anchor link (replace slashes and spaces with hyphens, remove .md)
            anchor = str(relative_path).replace('/', '-').replace(' ', '-').replace('.md', '').lower()
            outfile.write(f"- [{relative_path}](#{anchor})\n")
        outfile.write("\n---\n\n")
        
        # Combine all markdown files
        for md_file in md_files:
            relative_path = md_file.relative_to(docs_path)
            print(f"Processing: {relative_path}")
            
            # Create section header
            outfile.write(f"\n\n## {relative_path}\n\n")
            outfile.write(f"**Source:** `{docs_dir}/{relative_path}`\n\n")
            outfile.write("---\n\n")
            
            # Read and write the content
            try:
                with open(md_file, 'r', encoding='utf-8') as infile:
                    content = infile.read()
                    outfile.write(content)
                    outfile.write("\n\n")
            except Exception as e:
                print(f"Error reading {md_file}: {e}")
                outfile.write(f"*Error reading file: {e}*\n\n")
            
            # Add separator between files
            outfile.write("\n---\n\n")
    
    print(f"\n✓ Successfully combined {len(md_files)} files into '{output_path}'")
    print(f"✓ Total size: {output_path.stat().st_size:,} bytes")


if __name__ == "__main__":
    combine_markdown_files()
