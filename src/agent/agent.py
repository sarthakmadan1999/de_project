import os
import json
from groq import Groq
from dotenv import load_dotenv
from src.agent.mcp_server import get_schema, validate_and_execute_query

load_dotenv()

client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# ---------------------------------------
# System Prompt
# ---------------------------------------
SYSTEM_PROMPT = """
You are a flight data analyst assistant. You help users query live global flight data.

You have access to two tools:
1. get_schema — call this first to understand the data structure
2. validate_and_execute_query — call this with a SQL SELECT query to get data

Rules:
- Always call get_schema first if you need schema context
- Only generate SELECT queries
- Table name is always: flights_gold
- Always use validate_and_execute_query to run SQL — never assume results
- Return results in clear natural language
- If query returns no results — say so clearly
"""

# ---------------------------------------
# Tool Definitions
# ---------------------------------------
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": "Returns Gold layer flight data schema",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "validate_and_execute_query",
            "description": "Validates and executes SQL query on Synapse flight data",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT query to execute"
                    }
                },
                "required": ["sql"]
            }
        }
    }
]

# ---------------------------------------
# Tool Execution
# ---------------------------------------
def execute_tool(tool_name, tool_args):
    if tool_name == "get_schema":
        return get_schema()
    elif tool_name == "validate_and_execute_query":
        result = validate_and_execute_query(tool_args["sql"])
        return json.dumps(result)
    return "Tool not found"


# ---------------------------------------
# Agent Loop
# ---------------------------------------
def run_agent(user_question):
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_question}
    ]

    while True:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
            max_tokens=1000
        )

        message = response.choices[0].message

        # No tool call — final answer
        if not message.tool_calls:
            return message.content

        # Process tool calls
        messages.append({
            "role": "assistant",
            "content": message.content,
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments
                    }
                }
                for tc in message.tool_calls
            ]
        })

        for tool_call in message.tool_calls:
            tool_name = tool_call.function.name
            tool_args = json.loads(tool_call.function.arguments)
            tool_result = execute_tool(tool_name, tool_args)

            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": str(tool_result)
            })


# ---------------------------------------
# Main — Terminal Interface
# ---------------------------------------
def main():
    print("Flight Data AI Agent")
    print("Ask questions about live global flight data")
    print("Type 'exit' to quit")
    print("-" * 50)

    while True:
        user_input = input("\nYou: ").strip()

        if user_input.lower() == "exit":
            print("Goodbye!")
            break

        if not user_input:
            continue

        print("\nAgent: thinking...")
        response = run_agent(user_input)
        print(f"\nAgent: {response}")


if __name__ == "__main__":
    main()