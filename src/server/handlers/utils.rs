// server/handlers/utils.rs - Utility functions for handlers

/// Helper function to determine if filters target conversations or organizations
pub fn is_targeting_conversations_or_organizations(filters: &[String]) -> bool {
    filters.iter().any(|filter| {
        let normalized = if filter.starts_with('/') {
            filter.clone()
        } else {
            format!("/{}", filter)
        };

        normalized.contains("/conversation") || normalized.contains("/organization")
    })
}