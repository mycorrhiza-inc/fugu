// Test integration module

// Make the shared test fixture visible to all test files
mod shared_test_fixture;

// Call the shutdown function when tests are complete
#[cfg(test)]
mod test_cleanup {
    use super::shared_test_fixture::shutdown_server;
    
    // This struct will call shutdown_server when dropped (at the end of all tests)
    pub struct CleanupServer;
    
    impl Drop for CleanupServer {
        fn drop(&mut self) {
            shutdown_server();
        }
    }
    
    // Create a static instance that will be dropped at the end of the test run
    #[ctor::ctor]
    fn init() {
        static CLEANUP: CleanupServer = CleanupServer;
    }
}