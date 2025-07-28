import asyncio
import random
from typing import Dict, Any
from datetime import datetime


class MockLLMService:
    """
    Mock LLM service that simulates AI responses for testing and development
    """
    
    def __init__(self):
        self.mock_responses = [
            "Thank you for your message! I'm a mock AI assistant helping you with your data analysis needs.",
            "Based on your data source, I can help you explore and analyze your information effectively.",
            "That's an interesting question! Let me provide you with some insights based on the available data.",
            "I understand your request. Here's how I can assist you with your data analysis task.",
            "Great question! Your data source contains valuable information that we can explore together.",
            "I'm here to help you make sense of your data. What specific aspects would you like to analyze?",
            "Thanks for using our chat system! I can help you discover patterns and insights in your data.",
            "Your message has been received. I'm processing your request and will provide helpful analysis.",
            "Excellent! Based on your data source configuration, I can offer several analytical approaches.",
            "I appreciate your query. Let me help you unlock the potential of your data through intelligent analysis."
        ]
        
        self.context_aware_responses = {
            "csv": "I can see you're working with CSV data. This format is excellent for structured data analysis, and I can help you explore patterns, perform calculations, and generate insights.",
            "xlsx": "You're using an Excel file as your data source. I can help you analyze spreadsheet data, work with multiple sheets, and extract meaningful information.",
            "postgres": "I notice you're connected to a PostgreSQL database. I can help you query your relational data and perform complex analytical operations.",
            "mysql": "You're working with MySQL data. I can assist with database queries, joins, and analytical functions to help you understand your data better.",
            "mongodb": "I see you're using MongoDB as your data source. I can help you work with document-based data and perform aggregations and analysis.",
            "google": "You're connected to Google services. I can help you analyze and work with your cloud-based data efficiently.",
            "pdf": "I notice you're working with PDF documents. I can help extract insights and information from your document-based data source."
        }
    
    def _calculate_token_count(self, text: str) -> int:
        """
        Mock token calculation - roughly 4 characters per token
        In a real implementation, you'd use a proper tokenizer
        """
        return max(1, len(text) // 4)
    
    def _get_context_response(self, data_source_type: str) -> str:
        """Get a context-aware response based on data source type"""
        return self.context_aware_responses.get(
            data_source_type.lower(), 
            self.mock_responses[0]
        )
    
    async def generate_response(
        self, 
        message: str, 
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generate a mock AI response
        
        Args:
            message: User message content
            context: Additional context like data source info, conversation history
            
        Returns:
            Dictionary with response content and metadata
        """
        # Simulate processing time
        await asyncio.sleep(random.uniform(0.5, 2.0))
        
        # Choose response based on context or randomly
        if context and context.get("data_source_type"):
            response_content = self._get_context_response(context["data_source_type"])
        else:
            response_content = random.choice(self.mock_responses)
        
        # Add some contextual elements to make it feel more realistic
        if "analyze" in message.lower():
            response_content = f"I'll help you analyze that data. {response_content}"
        elif "?" in message:
            response_content = f"That's a great question! {response_content}"
        elif len(message) > 100:
            response_content = f"I see you've provided detailed information. {response_content}"
        
        # Calculate token count
        token_count = self._calculate_token_count(response_content)
        
        return {
            "content": response_content,
            "token_count": token_count,
            "model": "mock-llm-v1",
            "processing_time": random.uniform(0.5, 2.0),
            "timestamp": datetime.utcnow()
        }
    
    async def generate_response_with_context(
        self, 
        message: str,
        conversation_history: list = None,
        data_source_info: Dict[str, Any] = None,
        user_preferences: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generate response with full context (for more sophisticated interactions)
        
        Args:
            message: Current user message
            conversation_history: Previous messages in the conversation
            data_source_info: Information about the connected data source
            user_preferences: User-specific preferences or settings
            
        Returns:
            Dictionary with response content and metadata
        """
        context = {}
        
        if data_source_info:
            context["data_source_type"] = data_source_info.get("type")
            context["data_source_name"] = data_source_info.get("name")
        
        if conversation_history:
            context["conversation_length"] = len(conversation_history)
            context["previous_topics"] = [msg.get("content", "")[:50] for msg in conversation_history[-3:]]
        
        # Generate base response
        response = await self.generate_response(message, context)
        
        # Add context-specific enhancements
        if conversation_history and len(conversation_history) > 5:
            response["content"] = f"Continuing our discussion, {response['content'].lower()}"
        
        if data_source_info:
            response["data_source_referenced"] = data_source_info.get("name", "Unknown")
        
        return response
    
    def calculate_token_count(self, text: str) -> int:
        """
        Public method to calculate token count for any text
        Useful for other parts of the system
        """
        return self._calculate_token_count(text)
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Health check endpoint for the mock LLM service
        """
        return {
            "status": "healthy",
            "service": "mock-llm",
            "version": "1.0.0",
            "available_models": ["mock-llm-v1"],
            "max_tokens_per_request": 4000,
            "timestamp": datetime.utcnow()
        }


