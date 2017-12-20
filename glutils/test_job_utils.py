import unittest

from .job_utils import code_format, zipped_b64_to_string, get_dynamodb_value, ip_address


class TestJobUtilFunctions(unittest.TestCase):

    def test_code_format_converts_normal_to_code(self):
        self.assertEqual('FOO_BAR_BAZ', code_format('Foo Bar Baz'))

    def test_code_format_works_with_nulls(self):
        self.assertIsNone(code_format(None))

    def test_code_format_works_on_preformatted_codes(self):
        self.assertEqual('BAZ_BAR_FOO', code_format('BAZ_BAR_FOO'))

    def test_code_format_removes_trailing_whitespace(self):
        self.assertEqual('BAZ', code_format('  baz  '))

    def test_zipped_b64_to_string_converts_binary_encoded_zip_strings_to_text(self):
        initval = "H4sIAAAAAAACA82TbW/aMBDHv8rJrzopZUl4Crwz8Y1YC3FkO+3QNqEAKUTNCoJQwSq++xzG+rBpHS+qibywdOf/3f3ufPn8QMrdMiNd2yL5lHSJH3xUfiBEaBOLTObZ5DYz7nK1ySxynxYbIyUiMnc3eVZM7/N1Ps6LvNyR7gMZL7bX+bScH7IZI8jy2bw8WMt0ld2Va5avl0W6q1LepMU6q0rkxdTc/X6zt0hWZN9M0Kjianr23vorq3NmrM4rrO6ZsbqvsNbPjLX+J2uIlHE20n5MR4wrPxQqkWjYinScFUbAIUCJvSH4IlIYadACelhZmvoaGdCeSDQgS3yquYhoCCKOhdRJxDVHBYfQQUwlahH/nIuCK04BB5SHFsSBiBBoxL68FxI0ftIW8MgPE8ajPgyGcM0lhqgURMmghxL4B4iluOIMmQWJqlQ00eKScRpWhkY/iEQo+sPHrLHES4m+kCYGBiYX7aOqAYckYiiVNjrQAdWPTXJTTWiglYPxqi8Qpmwi/YAqrJGnh7R/zerlO5bZtvQXxWJlJKvZ+KLdtMBrWODYzXcmYpxObmerxeZu+kyUXrj1jgVuwzNHsxIfpG+4EdaBS+XfK3KnudyS/f/fwsZxYKOX7ubTcrrH5VQYMuxLRPPefUkHx6Wz//XrtDqNmmcm/hzddWotu/rqntNov0UfrZOBnfMAbp8M7J4HsHcycP08gDv7rz8AAN8FlBIIAAA="
        expected = '[{"type":0,"id":"CHKSCHOOL0","checked":true,"value":"ON","fieldvisibility":{"boxWidth":0,"boxHeight":0,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":580},{"type":0,"id":"CHKSCHOOL1","checked":true,"value":"ON","fieldvisibility":{"boxWidth":0,"boxHeight":0,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":581},{"type":0,"id":"CHKSCHOOL2","checked":true,"value":"ON","fieldvisibility":{"boxWidth":0,"boxHeight":0,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":582},{"type":0,"id":"CHKSCHOOL3","checked":true,"value":"ON","fieldvisibility":{"boxWidth":0,"boxHeight":0,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":583},{"type":0,"id":"LEADID_TCPA_DISCLOSURE","label":"I HEREBY CONSENT TO BE CONTACTED ABOUT EDUCATIONAL OPPORTUNITIES BY COMPARETOPSCHOOLS VIA EMAIL, PHONE AND\\/OR TEXT, INCLUDING MY WIRELESS NUMBER IF PROVIDED, USING AUTO-DIALING TECHNOLOGY AND\\/OR PRE-RECORDED MESSAGES. I UNDERSTAND THAT CONSENT IS NOT A CONDITION OF PURCHASE.","value":"0","labelvisibility":{"textColor":"rgb(75, 84, 105)","backgroundColor":"rgba(239, 248, 254, 1)","boxWidth":0,"boxHeight":0,"parentsDisplayed":false,"childrenDisplayed":false,"textSize":"15px"},"fieldvisibility":{"boxWidth":0,"boxHeight":0,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":584,"label_element_id":585},{"type":2,"id":"SELDEGREEPROGRAMCONTACT0","fieldvisibility":{"boxWidth":694.875,"boxHeight":21.60000038147,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":586},{"type":2,"id":"SELDEGREEPROGRAMCONTACT1","fieldvisibility":{"boxWidth":694.875,"boxHeight":21.60000038147,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":587},{"type":2,"id":"SELDEGREEPROGRAMCONTACT2","fieldvisibility":{"boxWidth":694.875,"boxHeight":21.60000038147,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":588},{"type":2,"id":"SELDEGREEPROGRAMCONTACT3","fieldvisibility":{"boxWidth":694.875,"boxHeight":21.60000038147,"parentsDisplayed":false,"childrenDisplayed":false},"element_id":589}]'
        self.assertEqual(expected, zipped_b64_to_string(initval))

    def test_zipped_b64_to_string_works_with_nulls(self):
        self.assertIsNone(zipped_b64_to_string(None))

    def test_get_dynamodb_value_works_with_nulls(self):
        self.assertIsNone(get_dynamodb_value(None))

# Test cases for DynamoDB Json values read
    def test_get_dynamodb_value_works_with_dynamo_string(self):
        initval = '{\"s\":\"A87D139E-74B1-0BCF-9188-73F369D69D24\"}'
        expected = "A87D139E-74B1-0BCF-9188-73F369D69D24"
        self.assertEqual(expected, get_dynamodb_value(initval))

    def test_get_dynamodb_value_works_with_dynamo_string(self):
        initval = '{"n":"1"}'
        expected = '1'
        self.assertEqual(expected, get_dynamodb_value(initval))

    def test_ip_address(self):
        initval = '3093326845'
        expected = '184.96.107.253'
        self.assertEqual(expected, ip_address(initval))


if __name__ == '__main__':
    unittest.main()
