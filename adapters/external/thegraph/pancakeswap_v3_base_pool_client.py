from __future__ import annotations

from typing import Any, Dict

from adapters.external.thegraph.thegraph_http_client import TheGraphHttpClient


class PancakeSwapV3BasePoolClient:
    """
    Client for PancakeSwap V3 on Base via The Graph Gateway.

    Subgraph:
      https://gateway.thegraph.com/api/subgraphs/id/BHWNsedAHtmTCzXxCCDfhPmm6iN9rxUhoRHdHKyujic3
    """

    SUBGRAPH_ENDPOINT = (
        "https://gateway.thegraph.com/api/subgraphs/id/"
        "BHWNsedAHtmTCzXxCCDfhPmm6iN9rxUhoRHdHKyujic3"
    )

    def __init__(self, *, api_key: str, timeout_s: float = 20.0) -> None:
        self._http = TheGraphHttpClient(
            endpoint=self.SUBGRAPH_ENDPOINT,
            api_key=api_key,
            timeout_s=timeout_s,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def get_pool(self, *, pool_address: str) -> Dict[str, Any]:
        """
        Fetch pool state by address.

        Note:
        - Schema fields may vary across subgraphs; this query uses common V3 fields.
        - If a field is missing, it will just not exist in the response.
        """
        q = """
        query Pool($id: ID!) {
          pool(id: $id) {
            id
            feeTier
            liquidity
            sqrtPrice
            tick
            token0Price
            token1Price
            volumeUSD
            totalValueLockedUSD
            token0 { id symbol decimals }
            token1 { id symbol decimals }
          }
        }
        """
        pool_id = str(pool_address).lower()
        res = await self._http.query(query=q, variables={"id": pool_id})
        return (res or {}).get("data", {}).get("pool") or {}
