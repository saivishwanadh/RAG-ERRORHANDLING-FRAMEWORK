from huggingface_hub import InferenceClient
import json
import logging
class EmbeddingGenerator:

    def __init__(self, api_key: str, provider: str = "hf-inference"):
        self.client = InferenceClient(
            provider=provider,
            api_key=api_key
        )

    def get_embedding(self, text: str, model: str = "google/embeddinggemma-300m"):
        """
        Generates embedding vector for the given text.
        """
        logging.info(f"Generating embedding using model: {model}")
        return self.client.feature_extraction(
            text,
            model=model
        )


# -------------------------------
# Example usage
# -------------------------------

'''if __name__ == "__main__":
    API_KEY = "API KEY"

    generator = EmbeddingGenerator(api_key=API_KEY)

    text = "Today is a sunny day and I will get some ice cream."

    embedding = generator.get_embedding(text)

    print(embedding)
    print("Vector length:", len(embedding))
    
    
    embed_payload={
        "embed_input": "Error:HTTP:INTERNAL_SERVER_ERROR Description:HTTP GET request failed due to internal server error"
    }

    embed_payload_json = json.dumps(embed_payload)
    embed_gen = EmbeddingGenerator(api_key=API_KEY)  # convert embed_input to vector using EmbeddingGenerator
    raw_embedding = embed_gen.get_embedding(embed_payload["embed_input"])
    print(raw_embedding)
    print("Vector length:", len(raw_embedding))'''
