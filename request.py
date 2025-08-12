#!/usr/bin/env python3
"""
Generic parallelized batch downloader.
Handles batch processing with configurable delays and concurrent requests.
Supports different content types (JSON, KML, etc.) with appropriate validation.
"""

import asyncio
import aiohttp
import json
import os
import sys
import random
import argparse
import ssl
from pathlib import Path
from typing import List, Tuple
import time

class ParallelDownloader:
    def __init__(self, min_batch_size=5, max_batch_size=20, min_delay=1.0, max_delay=5.0, max_concurrent=10, content_type='json', http_method='POST'):
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_concurrent = max_concurrent
        self.content_type = content_type.lower()
        self.http_method = http_method.upper()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Stats tracking
        self.downloaded = 0
        self.skipped = 0
        self.failed = 0
        
    def parse_url_file(self, url_file_path: str) -> List[Tuple[str, str]]:
        """Parse URL file with format: url output_path"""
        urls_and_paths = []
        
        try:
            with open(url_file_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    parts = line.split('\t')
                    if len(parts) != 2:
                        print(f"Warning: Skipping malformed line {line_num}: {line}")
                        continue
                    
                    url, output_path = parts
                    urls_and_paths.append((url.strip(), output_path.strip()))
                    
        except FileNotFoundError:
            print(f"Error: URL file {url_file_path} not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error reading URL file: {e}")
            sys.exit(1)
            
        return urls_and_paths
    
    def validate_file_content(self, file_path: str) -> bool:
        """Validate file content based on content type"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if self.content_type == 'json':
                json.loads(content)
                return True
            elif self.content_type == 'kml':
                # Basic KML validation - check for KML tags
                return '<kml' in content.lower() or '<placemark' in content.lower()
            else:
                # For other content types, just check if file has content
                return len(content.strip()) > 0
                
        except (json.JSONDecodeError, IOError, UnicodeDecodeError):
            return False
    
    def filter_existing_files(self, urls_and_paths: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """Filter out URLs where output files already exist and are valid"""
        filtered = []
        
        for url, output_path in urls_and_paths:
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                if self.validate_file_content(output_path):
                    self.skipped += 1
                    continue
            
            filtered.append((url, output_path))
        
        return filtered
    
    async def download_single(self, session: aiohttp.ClientSession, url: str, output_path: str) -> bool:
        """Download a single file with retry logic"""
        async with self.semaphore:
            temp_path = f"{output_path}.tmp"
            
            try:
                # Ensure output directory exists
                output_dir = os.path.dirname(output_path)
                if output_dir:  # Only create if there's actually a directory part
                    os.makedirs(output_dir, exist_ok=True)
                
                # Use appropriate HTTP method
                request_method = session.post if self.http_method == 'POST' else session.get
                async with request_method(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        
                        # Validate content based on content type
                        if self.content_type == 'json':
                            try:
                                json.loads(content)
                            except json.JSONDecodeError:
                                print(f"Warning: Invalid JSON from {url}")
                                return False
                        elif self.content_type == 'kml':
                            if not ('<kml' in content.lower() or '<placemark' in content.lower()):
                                print(f"Warning: Invalid KML content from {url}")
                                return False
                        
                        # Write to temp file first, then move
                        with open(temp_path, 'w') as f:
                            f.write(content)
                        
                        os.rename(temp_path, output_path)
                        self.downloaded += 1
                        return True
                    else:
                        print(f"HTTP {response.status} for {url}")
                        return False
                        
            except Exception as e:
                print(f"Error downloading {url}: {e}")
                # Clean up temp file
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return False
    
    async def download_batch(self, session: aiohttp.ClientSession, batch: List[Tuple[str, str]]):
        """Download a batch of files concurrently"""
        tasks = []
        for url, output_path in batch:
            task = asyncio.create_task(self.download_single(session, url, output_path))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count failures
        for result in results:
            if isinstance(result, Exception) or result is False:
                self.failed += 1
    
    async def process_downloads(self, urls_and_paths: List[Tuple[str, str]]):
        """Process all downloads in randomized batches"""
        if not urls_and_paths:
            print("No files to download")
            return
        
        print(f"Processing {len(urls_and_paths)} downloads...")
        
        # Create session with reasonable timeout and SSL context
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        # Create SSL context that doesn't verify certificates (like curl without --cacert)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=20, ssl=ssl_context)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # Process in random batches
            remaining = urls_and_paths.copy()
            batch_num = 0
            
            while remaining:
                batch_num += 1
                # Ensure batch size is valid - min_batch_size cannot exceed remaining files
                effective_min_batch = min(self.min_batch_size, len(remaining))
                effective_max_batch = min(self.max_batch_size, len(remaining))
                batch_size = random.randint(effective_min_batch, effective_max_batch)
                
                # Take random sample for this batch
                batch = random.sample(remaining, batch_size)
                for item in batch:
                    remaining.remove(item)
                
                print(f"Batch {batch_num}: Processing {len(batch)} files ({len(remaining)} remaining)")
                
                start_time = time.time()
                await self.download_batch(session, batch)
                batch_time = time.time() - start_time
                
                print(f"  Batch completed in {batch_time:.1f}s")
                
                # Random delay between batches (except for the last batch)
                if remaining:
                    delay = random.uniform(self.min_delay, self.max_delay)
                    print(f"  Waiting {delay:.1f}s before next batch...")
                    await asyncio.sleep(delay)

def main():
    parser = argparse.ArgumentParser(description='Generic parallel batch downloader')
    parser.add_argument('url_file', help='File containing URLs and output paths (tab-separated)')
    parser.add_argument('--min-batch-size', type=int, default=5, help='Minimum batch size (default: 5)')
    parser.add_argument('--max-batch-size', type=int, default=20, help='Maximum batch size (default: 20)')
    parser.add_argument('--min-delay', type=float, default=1.0, help='Minimum delay between batches in seconds (default: 1.0)')
    parser.add_argument('--max-delay', type=float, default=5.0, help='Maximum delay between batches in seconds (default: 5.0)')
    parser.add_argument('--max-concurrent', type=int, default=10, help='Maximum concurrent downloads (default: 10)')
    parser.add_argument('--content-type', type=str, default='json', choices=['json', 'kml'], help='Content type for validation (default: json)')
    parser.add_argument('--http-method', type=str, default='POST', choices=['GET', 'POST'], help='HTTP method to use (default: POST)')
    
    args = parser.parse_args()
    
    downloader = ParallelDownloader(
        min_batch_size=args.min_batch_size,
        max_batch_size=args.max_batch_size,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_concurrent=args.max_concurrent,
        content_type=args.content_type,
        http_method=args.http_method
    )
    
    # Parse URLs
    print(f"Reading URLs from {args.url_file}...")
    urls_and_paths = downloader.parse_url_file(args.url_file)
    print(f"Found {len(urls_and_paths)} URLs")
    
    # Filter existing files
    print("Filtering existing valid files...")
    urls_to_download = downloader.filter_existing_files(urls_and_paths)
    print(f"Skipped {downloader.skipped} existing files")
    print(f"Need to download {len(urls_to_download)} files")
    
    if urls_to_download:
        # Run the downloads
        asyncio.run(downloader.process_downloads(urls_to_download))
    
    # Print final stats
    print("\nDownload Summary:")
    print(f"  Downloaded: {downloader.downloaded}")
    print(f"  Skipped (existing): {downloader.skipped}")
    print(f"  Failed: {downloader.failed}")
    print(f"  Total processed: {downloader.downloaded + downloader.skipped + downloader.failed}")

if __name__ == "__main__":
    main()