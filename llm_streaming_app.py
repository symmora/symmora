# llm_streaming_app.py
from ctransformers import AutoModelForCausalLM, LLM
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

def _sync_streaming_producer(llm: LLM, contents: str, queue: asyncio.Queue):
    """
    Synchronous producer that runs in a separate thread.
    It iterates over the blocking generator and puts tokens into the async queue.
    """
    try:
        for token in llm(contents, stream=True, max_new_tokens=1000):
            # Put the token into the queue to be consumed by the async generator.
            queue.put_nowait(token)
    finally:
        # Put a sentinel value to signal the end of the stream.
        queue.put_nowait(None)

async def callback_stream(contents: str):
    """
    An async generator that yields tokens from the model as they are generated.
    It runs the blocking model generator in a separate thread and uses a queue
    to communicate tokens back to the asyncio event loop.
    """
    if "mistral" not in llms:
        # Run the blocking model loading in a separate thread.
        llms["mistral"] = await asyncio.to_thread(_load_model_sync)

    llm = llms["mistral"]
    queue = asyncio.Queue()

    # Start the synchronous producer in a background thread.
    # The loop argument is not needed in modern Python asyncio.
    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        None,  # Use the default thread pool executor
        _sync_streaming_producer,
        llm,
        contents,
        queue
    )

    # Consume items from the queue until the sentinel value is received.
    while True:
        token = await queue.get()
        if token is None:
            break
        yield token

async def main():
    print("Starting LLM streaming application.")
    prompt = "Tell me a short story about a robot who discovers music."
    print(f"User prompt: {prompt}\n")
    print("--- Model Response (streaming) ---")

    # Use an async for loop to iterate over the tokens from the corrected async generator.
    async for token in callback_stream(prompt):
        print(token, end='', flush=True)

    print("\n------------------------------------")
    print("Streaming complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting application.")