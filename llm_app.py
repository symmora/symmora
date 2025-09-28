# llm_app.py
from ctransformers import AutoModelForCausalLM
import asyncio

# Model cache
llms = {}

async def callback(contents: str):
    if "mistral" not in llms:
        print("Loading Mistral model...")
        llms["mistral"] = AutoModelForCausalLM.from_pretrained(
            "TheBloke/Mistral-7B-Instruct-v0.1-GGUF",
            model_file="mistral-7b-instruct-v0.1.Q4_K_M.gguf",
            model_type="mistral"
        )
        print("Model loaded.")

    llm = llms["mistral"]
    response = llm(contents, stream=True, max_new_tokens=1000)
    message = ""

    for token in response:
        message += token
        yield message

async def main():
    print("Starting LLM application.")
    prompt = "What is the capital of France?"
    print(f"User prompt: {prompt}")

    final_message = ""
    async for message_chunk in callback(prompt):
        # In a real application, you might send this chunk to a websocket
        # For this example, we'll just print the latest full message
        final_message = message_chunk

    print("\n--- Final Response ---")
    print(final_message)
    print("----------------------")

if __name__ == "__main__":
    # This setup is needed to run the async main function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting application.")