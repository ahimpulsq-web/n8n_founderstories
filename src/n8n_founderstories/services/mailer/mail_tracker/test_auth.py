#!/usr/bin/env python3
"""
Test script for mail tracker authentication endpoint.

This script tests the authentication endpoint by sending requests
with valid and invalid credentials.

Usage:
    python test_auth.py [--host HOST] [--port PORT]
"""

import argparse
import requests
import json
from typing import Dict, Any


def test_auth(host: str = "127.0.0.1", port: int = 8000) -> None:
    """
    Test the authentication endpoint with various scenarios.
    
    Args:
        host: API host (default: 127.0.0.1)
        port: API port (default: 8000)
    """
    base_url = f"http://{host}:{port}/api/v1/mailer/auth"
    
    print(f"\n{'='*60}")
    print(f"Testing Authentication Endpoint: {base_url}")
    print(f"{'='*60}\n")
    
    # Test cases
    test_cases = [
        {
            "name": "Valid credentials (admin/admin)",
            "username": "admin",
            "password": "admin",
            "expected_auth": True
        },
        {
            "name": "Invalid password",
            "username": "admin",
            "password": "wrongpassword",
            "expected_auth": False
        },
        {
            "name": "Invalid username",
            "username": "nonexistent",
            "password": "admin",
            "expected_auth": False
        },
        {
            "name": "Empty credentials",
            "username": "",
            "password": "",
            "expected_auth": False
        }
    ]
    
    results = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"Test {i}: {test_case['name']}")
        print(f"  Username: '{test_case['username']}'")
        print(f"  Password: '{test_case['password']}'")
        
        try:
            response = requests.post(
                base_url,
                json={
                    "username": test_case["username"],
                    "password": test_case["password"]
                },
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            
            print(f"  Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"  Response: {json.dumps(data, indent=4)}")
                
                # Verify expected result
                if data["authenticated"] == test_case["expected_auth"]:
                    print(f"  ✓ PASSED - Authentication result matches expected")
                    results.append(("PASS", test_case["name"]))
                else:
                    print(f"  ✗ FAILED - Expected authenticated={test_case['expected_auth']}, got {data['authenticated']}")
                    results.append(("FAIL", test_case["name"]))
            else:
                print(f"  Response: {response.text}")
                print(f"  ✗ FAILED - Unexpected status code")
                results.append(("FAIL", test_case["name"]))
        
        except requests.exceptions.ConnectionError:
            print(f"  ✗ ERROR - Could not connect to {base_url}")
            print(f"  Make sure the API server is running on {host}:{port}")
            results.append(("ERROR", test_case["name"]))
        
        except Exception as e:
            print(f"  ✗ ERROR - {str(e)}")
            results.append(("ERROR", test_case["name"]))
        
        print()
    
    # Summary
    print(f"{'='*60}")
    print("Test Summary")
    print(f"{'='*60}\n")
    
    passed = sum(1 for status, _ in results if status == "PASS")
    failed = sum(1 for status, _ in results if status == "FAIL")
    errors = sum(1 for status, _ in results if status == "ERROR")
    
    for status, name in results:
        symbol = "✓" if status == "PASS" else "✗"
        print(f"  {symbol} {status}: {name}")
    
    print(f"\nTotal: {len(results)} tests")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Errors: {errors}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Test mail tracker authentication endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--host',
        default='127.0.0.1',
        help='API host (default: 127.0.0.1)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=8000,
        help='API port (default: 8000)'
    )
    
    args = parser.parse_args()
    
    test_auth(args.host, args.port)


if __name__ == '__main__':
    main()