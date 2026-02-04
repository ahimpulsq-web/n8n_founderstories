from n8n_founderstories.services.embeddings.match_industry import match_industries

if __name__ == "__main__":
    prompt_target = "SaaS startups"

    industries = match_industries(
        prompt_target=prompt_target,
        top_k=10,
    )

    print("Prompt target:", prompt_target)
    print("Matched industries:")
    for i, ind in enumerate(industries, 1):
        print(f"{i:02d}. {ind}")
