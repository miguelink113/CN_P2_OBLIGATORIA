import requests
import json

url = "https://intranet.rfevb.com/webservices/rfevbcom/vplaya/vp-ranking-masculino.php?fechaHasta=2025-12-29&buscar="

def save_ranking():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.rfevb.com/"
    }

    try:
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'
        
        # Directly parse the JSON response
        data = response.json()
        
        # Save it to a file
        with open('ranking_nacional_voleyplaya.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            
        print(f"Successfully saved {len(data)} players to ranking_nacional_voleyplaya.json")
        
        # Optional: Print the top 3 to verify
        print("\nTop 3 Players:")
        for i in range(min(3, len(data))):
            p = data[i]
            print(f"{i+1}. {p['ApellidosNombre']} - {p['Puntos']} pts")

    except Exception as e:
        print(f"Error: {e}")

save_ranking()