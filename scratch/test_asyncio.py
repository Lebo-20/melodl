from merge import merge_episodes
import asyncio

async def test():
    print("Testing asyncio in merge...")
    # We won't call it since it needs files, but we check if it exists in the module
    import merge
    print(f"asyncio in merge module: {getattr(merge, 'asyncio', 'NOT FOUND')}")

asyncio.run(test())
