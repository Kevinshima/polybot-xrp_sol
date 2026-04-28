#!/usr/bin/env python3
"""
One-time script: approve USDC and CTF contracts for Polymarket trading.
Run this before the bot can place any orders.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from config import settings

# ERC-20 minimal ABI for approve()
ERC20_ABI = [
    {
        "name": "approve",
        "type": "function",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
    },
    {
        "name": "allowance",
        "type": "function",
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]

# CTF setApprovalForAll ABI
CTF_ABI = [
    {
        "name": "setApprovalForAll",
        "type": "function",
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "name": "isApprovedForAll",
        "type": "function",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
    },
]

MAX_UINT256 = 2**256 - 1


def main():
    if not settings.POLY_PRIVATE_KEY:
        print("ERROR: POLY_PRIVATE_KEY not set in .env")
        sys.exit(1)

    w3 = Web3(Web3.HTTPProvider(settings.POLYGON_RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    if not w3.is_connected():
        print(f"ERROR: Cannot connect to Polygon RPC at {settings.POLYGON_RPC_URL}")
        sys.exit(1)

    account = w3.eth.account.from_key(settings.POLY_PRIVATE_KEY)
    wallet = account.address
    print(f"Wallet: {wallet}")

    # ── USDC balance ──────────────────────────────────────────────────────────
    usdc = w3.eth.contract(
        address=Web3.to_checksum_address(settings.USDC_ADDRESS),
        abi=ERC20_ABI,
    )
    balance_raw = usdc.functions.balanceOf(wallet).call()
    balance_usdc = balance_raw / 1e6  # USDC has 6 decimals
    print(f"USDC Balance: {balance_usdc:.2f}")

    # ── Approve USDC → CLOB Exchange ──────────────────────────────────────────
    clob_addr = Web3.to_checksum_address(settings.CLOB_EXCHANGE_ADDRESS)
    allowance_raw = usdc.functions.allowance(wallet, clob_addr).call()
    allowance_usdc = allowance_raw / 1e6

    if allowance_usdc < 1_000_000:
        print(f"Approving USDC for CLOB Exchange ({settings.CLOB_EXCHANGE_ADDRESS})…")
        nonce = w3.eth.get_transaction_count(wallet)
        tx = usdc.functions.approve(clob_addr, MAX_UINT256).build_transaction({
            "from": wallet,
            "nonce": nonce,
            "gas": 100_000,
            "gasPrice": w3.eth.gas_price,
            "chainId": settings.CHAIN_ID,
        })
        signed = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"✓ USDC approved: https://polygonscan.com/tx/{tx_hash.hex()}")
    else:
        print(f"✓ USDC already approved ({allowance_usdc:.0f} USDC)")

    # ── CTF setApprovalForAll → CLOB Exchange ────────────────────────────────
    ctf = w3.eth.contract(
        address=Web3.to_checksum_address(settings.CTF_ADDRESS),
        abi=CTF_ABI,
    )
    is_approved = ctf.functions.isApprovedForAll(wallet, clob_addr).call()

    if not is_approved:
        print(f"Setting CTF ApprovalForAll for CLOB Exchange…")
        nonce = w3.eth.get_transaction_count(wallet)
        tx = ctf.functions.setApprovalForAll(clob_addr, True).build_transaction({
            "from": wallet,
            "nonce": nonce,
            "gas": 100_000,
            "gasPrice": w3.eth.gas_price,
            "chainId": settings.CHAIN_ID,
        })
        signed = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"✓ CTF approved: https://polygonscan.com/tx/{tx_hash.hex()}")
    else:
        print("✓ CTF already approved")

    print("\nSetup complete! You can now run the bot.")


if __name__ == "__main__":
    main()
