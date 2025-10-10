#!/bin/bash
# DriftDB Security Testing Script
# Run automated security tests against DriftDB

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

HOST="${DRIFTDB_HOST:-localhost}"
PORT="${DRIFTDB_PORT:-5432}"
BASE_URL="http://$HOST:$PORT"

echo "========================================="
echo "DriftDB Security Test Suite"
echo "========================================="
echo "Target: $BASE_URL"
echo ""

# Test counter
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

run_test() {
    local test_name="$1"
    local test_cmd="$2"
    local expected="$3"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -n "[$TOTAL_TESTS] Testing: $test_name... "

    if eval "$test_cmd"; then
        echo -e "${GREEN}PASS${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# ========================================
# Authentication Security Tests
# ========================================
echo ""
echo "=== Authentication Tests ==="

# Test 1: Reject weak passwords
run_test "Weak password rejection" \
    "curl -s -X POST $BASE_URL/signup -d 'username=test&password=123' | grep -q 'password too weak'" \
    "Should reject weak passwords"

# Test 2: Rate limiting on login attempts
run_test "Login rate limiting" \
    "for i in {1..20}; do curl -s -X POST $BASE_URL/login -d 'username=admin&password=wrong' &>/dev/null; done; \
     curl -s -X POST $BASE_URL/login -d 'username=admin&password=test' | grep -q 'rate limit'" \
    "Should rate limit after multiple failed attempts"

# Test 3: Session token randomness
run_test "Session token randomness" \
    "token1=\$(curl -s -X POST $BASE_URL/login -d 'username=user1&password=pass1' | jq -r .token); \
     token2=\$(curl -s -X POST $BASE_URL/login -d 'username=user2&password=pass2' | jq -r .token); \
     [ \"\$token1\" != \"\$token2\" ]" \
    "Session tokens should be unique"

# Test 4: Session expiration
run_test "Session expiration" \
    "token=\$(curl -s -X POST $BASE_URL/login -d 'username=test&password=test' | jq -r .token); \
     sleep 3700; \
     curl -s -H \"Authorization: Bearer \$token\" $BASE_URL/users | grep -q 'expired'" \
    "Sessions should expire after timeout"

# Test 5: Password hash not exposed
run_test "Password hash protection" \
    "curl -s -X GET $BASE_URL/users/1 | jq .password_hash | grep -q 'null'" \
    "Password hashes should never be exposed"

# ========================================
# Authorization Security Tests
# ========================================
echo ""
echo "=== Authorization Tests ==="

# Test 6: RBAC enforcement
run_test "RBAC permission checking" \
    "user_token=\$(curl -s -X POST $BASE_URL/login -d 'username=user&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$user_token\" -X POST $BASE_URL/admin/users | grep -q '403'" \
    "Non-admin users should be denied admin endpoints"

# Test 7: Privilege escalation prevention
run_test "Privilege escalation prevention" \
    "user_token=\$(curl -s -X POST $BASE_URL/login -d 'username=user&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$user_token\" -X POST $BASE_URL/grant_role -d 'username=user&role=admin' | grep -q '403'" \
    "Users should not be able to grant themselves admin"

# Test 8: Horizontal privilege escalation
run_test "Horizontal privilege escalation" \
    "alice_token=\$(curl -s -X POST $BASE_URL/login -d 'username=alice&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$alice_token\" $BASE_URL/users/bob/data | grep -q '403'" \
    "Users should not access other users' data"

# Test 9: Resource ownership validation
run_test "Resource ownership" \
    "user_token=\$(curl -s -X POST $BASE_URL/login -d 'username=user&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$user_token\" -X DELETE $BASE_URL/documents/999 | grep -q 'not found\\|forbidden'" \
    "Users should only delete their own resources"

# ========================================
# Input Validation Tests
# ========================================
echo ""
echo "=== Input Validation Tests ==="

# Test 10: SQL injection prevention
run_test "SQL injection - basic" \
    "curl -s -X POST $BASE_URL/query -d \"query=SELECT * FROM users WHERE username='admin' OR '1'='1'\" | \
     grep -q 'syntax error\\|injection detected'" \
    "Should reject SQL injection attempts"

# Test 11: SQL injection - union attack
run_test "SQL injection - UNION" \
    "curl -s -X POST $BASE_URL/query -d \"query=SELECT * FROM users UNION SELECT * FROM passwords\" | \
     grep -q 'syntax error\\|injection detected'" \
    "Should reject UNION-based SQL injection"

# Test 12: Command injection prevention
run_test "Command injection" \
    "curl -s -X POST $BASE_URL/backup -d 'path=/tmp/backup; rm -rf /' | \
     grep -q 'invalid\\|forbidden'" \
    "Should reject command injection attempts"

# Test 13: Path traversal prevention
run_test "Path traversal" \
    "curl -s -X GET $BASE_URL/files/../../../../etc/passwd | \
     grep -q '404\\|forbidden'" \
    "Should prevent path traversal attacks"

# Test 14: XSS prevention (if web interface exists)
run_test "XSS prevention" \
    "curl -s -X POST $BASE_URL/comment -d 'text=<script>alert(1)</script>' | \
     grep -q 'escaped\\|sanitized'" \
    "Should escape user input to prevent XSS"

# ========================================
# Session Management Tests
# ========================================
echo ""
echo "=== Session Management Tests ==="

# Test 15: Session invalidation on logout
run_test "Logout invalidates session" \
    "token=\$(curl -s -X POST $BASE_URL/login -d 'username=test&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$token\" -X POST $BASE_URL/logout; \
     curl -s -H \"Authorization: Bearer \$token\" $BASE_URL/users | grep -q 'invalid\\|expired'" \
    "Tokens should be invalid after logout"

# Test 16: Concurrent session limits
run_test "Concurrent session limits" \
    "for i in {1..10}; do curl -s -X POST $BASE_URL/login -d 'username=test&password=test' &; done; wait; \
     curl -s -X POST $BASE_URL/login -d 'username=test&password=test' | grep -q 'too many sessions'" \
    "Should limit concurrent sessions per user"

# Test 17: Session fixation prevention
run_test "Session fixation prevention" \
    "token=\$(curl -s -X GET $BASE_URL/session | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$token\" -X POST $BASE_URL/login -d 'username=test&password=test' | \
     jq -r .token | grep -v \"\$token\"" \
    "Should generate new token after login"

# ========================================
# TLS/Encryption Tests
# ========================================
echo ""
echo "=== TLS/Encryption Tests ==="

# Test 18: TLS version enforcement (requires openssl)
if command -v openssl &> /dev/null; then
    run_test "TLS 1.2 rejection" \
        "! openssl s_client -connect $HOST:$PORT -tls1_2 -brief 2>&1 | grep -q 'Cipher'" \
        "Should reject TLS 1.2 connections"

    run_test "TLS 1.3 acceptance" \
        "openssl s_client -connect $HOST:$PORT -tls1_3 -brief 2>&1 | grep -q 'Cipher'" \
        "Should accept TLS 1.3 connections"

    # Test 20: Weak cipher rejection
    run_test "Weak cipher rejection" \
        "! openssl s_client -connect $HOST:$PORT -cipher 'RC4' 2>&1 | grep -q 'Cipher'" \
        "Should reject weak ciphers"
else
    echo -e "${YELLOW}Skipping TLS tests (openssl not found)${NC}"
fi

# ========================================
# Rate Limiting Tests
# ========================================
echo ""
echo "=== Rate Limiting Tests ==="

# Test 21: Query rate limiting
run_test "Query rate limiting" \
    "token=\$(curl -s -X POST $BASE_URL/login -d 'username=test&password=test' | jq -r .token); \
     for i in {1..150}; do curl -s -H \"Authorization: Bearer \$token\" -X POST $BASE_URL/query -d 'query=SELECT 1' &>/dev/null; done; \
     curl -s -H \"Authorization: Bearer \$token\" -X POST $BASE_URL/query -d 'query=SELECT 1' | grep -q 'rate limit'" \
    "Should rate limit excessive queries"

# Test 22: Expensive query blocking
run_test "Expensive query blocking" \
    "token=\$(curl -s -X POST $BASE_URL/login -d 'username=test&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$token\" -X POST $BASE_URL/query \
     -d 'query=SELECT * FROM huge_table a CROSS JOIN huge_table b CROSS JOIN huge_table c' | \
     grep -q 'too expensive\\|rate limit'" \
    "Should block expensive queries"

# Test 23: Connection rate limiting
run_test "Connection rate limiting" \
    "for i in {1..100}; do curl -s -X GET $BASE_URL/ &>/dev/null & done; wait; \
     curl -s -X GET $BASE_URL/ | grep -q 'rate limit\\|too many connections'" \
    "Should limit connection rate per IP"

# ========================================
# Data Security Tests
# ========================================
echo ""
echo "=== Data Security Tests ==="

# Test 24: Row-level security enforcement
run_test "Row-level security" \
    "token1=\$(curl -s -X POST $BASE_URL/login -d 'username=tenant1&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$token1\" -X POST $BASE_URL/query \
     -d 'query=SELECT * FROM data WHERE tenant_id=tenant2' | jq .rows | grep -q '\\[\\]'" \
    "Should filter rows based on RLS policies"

# Test 25: Sensitive data masking
run_test "Sensitive data masking" \
    "curl -s -X GET $BASE_URL/users/1 | jq .ssn | grep -q 'XXX-XX-'" \
    "Should mask sensitive fields"

# Test 26: Data encryption verification
run_test "Data-at-rest encryption" \
    "strings /var/lib/driftdb/data/*.db | grep -q 'sensitive_value'; [ \$? -ne 0 ]" \
    "Data files should be encrypted"

# ========================================
# Audit Logging Tests
# ========================================
echo ""
echo "=== Audit Logging Tests ==="

# Test 27: Failed login logged
run_test "Failed login logging" \
    "curl -s -X POST $BASE_URL/login -d 'username=admin&password=wrong' &>/dev/null; \
     grep -q 'login_failed.*admin' /var/log/driftdb/audit.log" \
    "Failed logins should be logged"

# Test 28: Permission denial logged
run_test "Permission denial logging" \
    "token=\$(curl -s -X POST $BASE_URL/login -d 'username=user&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$token\" -X POST $BASE_URL/admin/users &>/dev/null; \
     grep -q 'permission_denied.*user' /var/log/driftdb/audit.log" \
    "Permission denials should be logged"

# Test 29: Data access logged
run_test "Data access logging" \
    "token=\$(curl -s -X POST $BASE_URL/login -d 'username=test&password=test' | jq -r .token); \
     curl -s -H \"Authorization: Bearer \$token\" $BASE_URL/sensitive_data &>/dev/null; \
     grep -q 'data_access.*sensitive_data' /var/log/driftdb/audit.log" \
    "Sensitive data access should be logged"

# ========================================
# Dependency Security Tests
# ========================================
echo ""
echo "=== Dependency Security Tests ==="

# Test 30: Known vulnerabilities
if command -v cargo &> /dev/null; then
    run_test "Dependency audit" \
        "cd .. && cargo audit 2>&1 | grep -q 'Crate:.*Vulnerabilities found'; [ \$? -ne 0 ]" \
        "Should have no known vulnerabilities"
else
    echo -e "${YELLOW}Skipping dependency tests (cargo not found)${NC}"
fi

# ========================================
# Results Summary
# ========================================
echo ""
echo "========================================="
echo "Security Test Results"
echo "========================================="
echo "Total Tests: $TOTAL_TESTS"
echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed: ${RED}$FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}All security tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some security tests failed. Review results above.${NC}"
    exit 1
fi
