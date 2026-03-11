"""
@author: Raja CSP Raman

source:
    ?
"""

import os
import requests
import re
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Load environment variables from .env file
load_dotenv()


# Adapter Pattern - Abstract base class for LLM adapters
class LLMAdapter(ABC):
    """Abstract adapter for different LLM providers"""
    
    @abstractmethod
    def get_client(self):
        """Return configured LLM client"""
        pass


# Concrete Adapters for different LLM providers
class OllamaAdapter(LLMAdapter):
    """Adapter for Ollama LLM provider"""
    
    def get_client(self):
        from langchain_ollama import ChatOllama
        
        model = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
        
        return ChatOllama(
            model=model,
            temperature=0.7,
            num_predict=512,
            timeout=120  # Increased timeout to 120 seconds
        )


class OpenAIAdapter(LLMAdapter):
    """Adapter for OpenAI LLM provider"""
    
    def get_client(self):
        from langchain_openai import ChatOpenAI
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
        
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        return ChatOpenAI(
            api_key=api_key,
            model=model,
            temperature=0.7,
            max_tokens=512,
            timeout=120  # Increased timeout to 120 seconds
        )


class LlamaCppAdapter(LLMAdapter):
    """Adapter for llama.cpp LLM provider (direct HTTP, no langchain-openai dependency)"""
    
    def __init__(self, base_url="http://127.0.0.1:8080/v1"):
        self.base_url = base_url
    
    def _check_server_health(self):
        """Check if llama.cpp server is running"""
        try:
            # Try to reach the health endpoint or models endpoint
            health_url = self.base_url.replace('/v1', '/health')
            response = requests.get(health_url, timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            try:
                # Fallback: try the models endpoint
                models_url = f"{self.base_url}/models"
                response = requests.get(models_url, timeout=5)
                return response.status_code == 200
            except requests.exceptions.RequestException:
                return False
    
    def get_client(self):
        """Return a simple HTTP client wrapper for llama.cpp"""
        if not self._check_server_health():
            raise ConnectionError(f"llama.cpp server is not running at {self.base_url}. Please start your llama.cpp server first.")
        
        # Return a simple wrapper that mimics langchain interface
        return LlamaCppClient(self.base_url)


class LlamaCppClient:
    """Simple HTTP client for llama.cpp that mimics langchain interface"""
    
    def __init__(self, base_url):
        self.base_url = base_url
    
    def invoke(self, prompt):
        """Send request to llama.cpp server"""
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                json={
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7,
                    "max_tokens": 512
                },
                timeout=180
            )
            response.raise_for_status()
            data = response.json()
            
            # Extract content from response
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            
            # Return object with content attribute (like langchain)
            class Response:
                def __init__(self, content):
                    self.content = content
            
            return Response(content)
            
        except Exception as e:
            raise Exception(f"llama.cpp request failed: {e}")


class GeminiAdapter(LLMAdapter):
    """Adapter for Google Gemini LLM provider"""
    
    def get_client(self):
        from langchain_google_genai import ChatGoogleGenerativeAI
        
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY environment variable is not set")
        
        model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        
        return ChatGoogleGenerativeAI(
            google_api_key=api_key,
            model=model,
            temperature=0.7,
            max_tokens=512,
            timeout=120
        )


# Factory Pattern - Creates appropriate LLM adapter based on provider
class LLMFactory:
    """Factory for creating LLM adapters based on provider type"""
    
    _adapters = {
        "ollama": OllamaAdapter,
        "openai": OpenAIAdapter,
        "llama.cpp": LlamaCppAdapter,
        "gemini": GeminiAdapter,
    }
    
    @classmethod
    def create_adapter(cls, provider: str) -> LLMAdapter:
        """Create and return appropriate LLM adapter"""
        provider = provider.lower().strip()
        
        if provider not in cls._adapters:
            available_providers = ", ".join(cls._adapters.keys())
            raise ValueError(f"Unsupported LLM provider: {provider}. Available providers: {available_providers}")
        
        return cls._adapters[provider]()
    
    @classmethod
    def register_adapter(cls, provider: str, adapter_class: type):
        """Register a new LLM adapter (for extensibility)"""
        cls._adapters[provider] = adapter_class


def get_llm():
    """Get LLM client based on environment configuration"""
    provider = os.getenv("LLM_PROVIDER", "ollama")
    print(f"Using LLM provider: {provider}")
    
    adapter = LLMFactory.create_adapter(provider)
    print(f"Successfully created adapter for: {provider}")
    return adapter.get_client()


# Tool Calling Functions
def detect_url_in_instructions(instructions: str) -> str:
    """
    Detect if instructions contain a URL
    
    Args:
        instructions: User instructions text
        
    Returns:
        URL string if found, None otherwise
    """
    if not instructions:
        return None
    
    url_pattern = r'https?://[^\s]+'
    match = re.search(url_pattern, instructions)
    return match.group(0) if match else None


def fetch_content_from_url(url: str) -> dict:
    """
    Fetch and parse content from URL using BeautifulSoup
    
    Args:
        url: URL to fetch content from
        
    Returns:
        dict with 'success', 'content', 'error' keys
    """
    try:
        print(f"📡 Fetching content from: {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(["script", "style", "nav", "footer", "header", "aside", "iframe"]):
            element.decompose()
        
        # Try to find main content areas
        content_areas = soup.find_all(
            ['article', 'main', 'div'], 
            class_=lambda x: x and any(keyword in x.lower() for keyword in ['content', 'article', 'post', 'entry', 'body'])
        )
        
        if not content_areas:
            # Fallback to body or entire document
            content_areas = [soup.body] if soup.body else [soup]
        
        text_content = []
        
        for area in content_areas:
            # Extract headings
            for heading in area.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                heading_text = heading.get_text(strip=True)
                if heading_text:
                    text_content.append(f"\n## {heading_text}\n")
            
            # Extract paragraphs
            for para in area.find_all('p'):
                para_text = para.get_text(strip=True)
                if para_text and len(para_text) > 10:  # Filter out very short paragraphs
                    text_content.append(para_text)
            
            # Extract code blocks
            for code in area.find_all(['pre', 'code']):
                code_text = code.get_text(strip=True)
                if code_text:
                    text_content.append(f"\n```\n{code_text}\n```\n")
            
            # Extract list items
            for ul in area.find_all(['ul', 'ol']):
                for li in ul.find_all('li', recursive=False):
                    li_text = li.get_text(strip=True)
                    if li_text:
                        text_content.append(f"• {li_text}")
        
        # Join and clean content
        full_content = "\n".join(text_content)
        
        # Remove excessive whitespace
        full_content = re.sub(r'\n{3,}', '\n\n', full_content)
        full_content = full_content.strip()
        
        if not full_content or len(full_content) < 100:
            return {
                'success': False,
                'content': None,
                'error': 'Insufficient content extracted from URL'
            }
        
        print(f"✅ Successfully fetched {len(full_content)} characters")
        
        return {
            'success': True,
            'content': full_content,
            'error': None
        }
        
    except requests.exceptions.Timeout:
        error_msg = f"Timeout while fetching URL: {url}"
        print(f"❌ {error_msg}")
        return {'success': False, 'content': None, 'error': error_msg}
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'content': None, 'error': error_msg}
        
    except Exception as e:
        error_msg = f"Error parsing content: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'content': None, 'error': error_msg}


def process_instructions_with_url(instructions: str) -> dict:
    """
    Process instructions and fetch content if URL is detected
    
    Args:
        instructions: User instructions text
        
    Returns:
        dict with 'has_url', 'url', 'content', 'enhanced_instructions' keys
    """
    url = detect_url_in_instructions(instructions)
    
    if not url:
        return {
            'has_url': False,
            'url': None,
            'content': None,
            'enhanced_instructions': instructions
        }
    
    print(f"🔍 URL detected in instructions: {url}")
    
    result = fetch_content_from_url(url)
    
    if result['success']:
        # Limit content size for LLM context (keep first 3000 chars)
        content = result['content']
        if len(content) > 3000:
            content = content[:3000] + "\n\n[Content truncated for length...]"
        
        enhanced_instructions = f"""Content fetched from: {url}

KNOWLEDGE BASE CONTENT:
{content}

Generate quiz questions based on the above content."""
        
        return {
            'has_url': True,
            'url': url,
            'content': result['content'],
            'enhanced_instructions': enhanced_instructions
        }
    else:
        print(f"⚠️ Failed to fetch content: {result['error']}")
        print(f"📝 Using original instructions")
        
        return {
            'has_url': True,
            'url': url,
            'content': None,
            'enhanced_instructions': instructions
        }

