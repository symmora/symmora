# llm_app.py
from ctransformers import AutoModelForCausalLM
import asyncio

# Model cache
llms = {}

def _load_model_sync():
    """Synchronous function to load the model, to be run in a thread."""
    print("Loading Mistral model...")
    model = AutoModelForCausalLM.from_pretrained(
        "TheBloke/Mistral-7B-Instruct-v0.1-GGUF",
        model_file="mistral-7b-instruct-v0.1.Q4_K_M.gguf",
        model_type="mistral"
    )
    print("Model loaded.")
    return model

def _generate_text_sync(llm, contents):
    """Synchronous function to generate text, to be run in a thread."""
    # This is a blocking call. We set stream=False to get the full response.
    response = llm(contents, stream=False, max_new_tokens=1000)
    return response

async def get_llm_response(contents: str) -> str:
    """
    Asynchronously gets a response from the LLM by running the blocking
    calls in a separate thread to avoid blocking the event loop.
    """
    if "mistral" not in llms:
        # Run the blocking model loading in a separate thread.
        llms["mistral"] = await asyncio.to_thread(_load_model_sync)

    llm = llms["mistral"]

    # Run the blocking text generation in a separate thread.
    response = await asyncio.to_thread(_generate_text_sync, llm, contents)
    return response

async def main():
    print("Starting LLM application.")
    prompt = "What is the capital of France?"
    print(f"User prompt: {prompt}")

    # Await the final message from the correctly implemented async function.
    final_message = await get_llm_response(prompt)

    print("\n--- Final Response ---")
    print(final_message)
    print("----------------------")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting application.")