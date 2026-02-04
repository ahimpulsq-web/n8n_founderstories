# test_classifier.py
from n8n_founderstories.services.web_search.openrouter_client import OpenRouterClient
from n8n_founderstories.services.web_search.llm_classifier.service import LLMWebsiteClassifier


def main():
    # Try FREE models first, then PREMIUM fallback
    client = OpenRouterClient.from_env(
        tier_env="LINK_CLASSIFIER_TIER",
        fallback_env="LLM_PREMIUM_MODELS",
    )
    clf = LLMWebsiteClassifier(client)

    real_serp_results = [
        {
            "url": "https://veganfitness.de/Bio-Protein",
            "title": "Bio Proteinpulver | Proteine in BIO-Qualität | 100% Vegan",
            "snippet": "Unsere natürlichen veganen Bio Proteine von VEGJi liefern hochwertige Aminosäuren und Nährstoffe. Sie stammen aus kontrolliert biologischem Anbau und ..."
        },
        {
            "url": "https://www.fairnatural.de/",
            "title": "Fairnatural® - Regionale Bio Nahrungsergänzungsmittel ohne ...",
            "snippet": "Auf der Suche nach dem richtigen Protein Pulver für deinen Eiweißshake bist du bei uns genau richtig – egal ob Vegan oder Whey Protein! Doch nicht nur bei ..."
        },
        {
            "url": "https://vetain.de/",
            "title": "Vetain® - Cleane Supplements & vegane Proteinpulver kaufen",
            "snippet": "100% vegan: Alle Vetain-Produkte sind pflanzlich und zu 100 % vegan. Vom klassischen Proteinpulver über die Vitamine für Veganer:innen bis hin zu Riegeln, ..."
        },
        {
            "url": "https://thefrankjuice.com/en/products/frank-bio-veganes-proteinpulver",
            "title": "Vegan organic protein powder made from just 5 ingredients",
            "snippet": "Your naturally vegan protein powder. Natural ingredients provide a natural flavour. Perfect for your next shake! » Buy online now."
        },
        {
            "url": "https://www.gq-magazin.de/body-care/galerie/vegane-proteinpulver-test",
            "title": "Veganes Proteinpulver im Test: Die 15 Testsieger laut Expert:innen",
            "snippet": "Zu den veganen Proteinpulvern ohne Soja zählen etwa das Vegan Protein von Bodylab oder das Vegan Protein 7k+ von Sunday Natural. ... Bio-Protein- ..."
        },
        {
            "url": "https://de.womensbest.com/products/vegan-protein-single",
            "title": "Vegan Protein | Women's Best",
            "snippet": "Pflanzenbasiertes Protein für den Muskelaufbau und -erhalt. Optimiert und besonders cremig. Angereichert mit wertvollen Ballaststoffen, DigeZyme® und wichtigem ..."
        },
        {
            "url": "https://orgainic.com/products/bio-vegan-protein",
            "title": "Bio Vegan Protein 700g - Natürlicher Geschmack Ohne Zusatzstoffe",
            "snippet": "Das Bio Vegan Protein aus gekeimten Reis- und Erbsenprotein von ORGAINIC ist frei von Zusatzstoffen und besteht aus nachhaltigen Inhaltsstoffen - komplett ..."
        },
        {
            "url": "https://peeroton.com/shop/high-protein-muskelfit/bio-vegan-protein-pulver-mix-500g-kakao-kakao-11634",
            "title": "BIO Vegan Protein Pulver Mix 500g Kakao - Peeroton",
            "snippet": "49% Proteingehalt, Vegan · Made in Austria, 100% Bio, für die Region aus der Region · Aus kontrollierter biologischer Landwirtschaft und CO2 neutral hergestellt ..."
        },
        {
            "url": "https://www.amazon.de/-/en/Ultimate-Protein-Organic-Sweeteners-%C3%96KO-039/dp/B08VL2C1YS",
            "title": "Ultimate Protein | 100% Organic Vegan | Natural | 1000g - Amazon.de",
            "snippet": "Amazon Brand - Amfit Nutrition Whey Protein Powder, Chocolate Hazelnut ... Die Qualität ist top - Bio, vegan ohne Zusatzstoffe und neutral im Geschmack."
        },
        {
            "url": "https://fitstream.eu/en/product/bio-vegan-protein/",
            "title": "Bio Vegan Protein (300g) - FitStream",
            "snippet": "Bio Vegan Protein has an exclusively plant-based formula with a natural protein content. It is free of chemical additives that burden the human body and harm ..."
        },
    ]

    results = clf.classify_many(real_serp_results, max_workers=5)
    for r in results:
        print("\n---")
        print("url:", r["url"])
        print("classification:", r["classification"])



if __name__ == "__main__":
    main()
