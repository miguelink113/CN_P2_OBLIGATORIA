import requests
import json
import os

url = "https://intranet.rfevb.com/webservices/rfevbcom/vplaya/vp-ranking-masculino.php?fechaHasta=2025-12-29&buscar="

def save_ranking(output_path='src/data/ranking_nacional_voleyplaya_rfevb.json'):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.rfevb.com/"
    }

    try:
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'

        # Directly parse the JSON response
        data = response.json()

        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save it to a file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"Successfully saved {len(data)} players to {output_path}")

        # Optional: Print the top 3 to verify
        print("\nTop 3 Players:")
        for i in range(min(3, len(data))):
            p = data[i]
            print(f"{i+1}. {p.get('ApellidosNombre', p.get('full_name', 'N/A'))} - {p.get('Puntos', p.get('ranking_points', 'N/A'))} pts")

        return data

    except Exception as e:
        print(f"Error: {e}")
        return None


if __name__ == '__main__':
    save_ranking()