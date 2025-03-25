from cypher_query_chain import (
    cypher_chain
)

def general_qa_tool(query: str) -> str:
    """Useful for answering general questions about orders, order details, shipper, customers, and products."""
    response = cypher_chain.invoke(query)

    return response.get("result")
