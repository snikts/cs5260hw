import unittest
import json
import consumer

class TestConsumer(unittest.TestCase):
    
    def setUp(self):
        print("hi")
        fromBucket = "usu-cs5260-sarahjones-bucket2"
        toBucket = "usu-cs5260-sarahjones-bucket3"
        self.app = consumer.Consumer(fromBucket, toBucket)
    
    def testCreateWidget(self):
        print("hi")
        jsonObjStr = '{"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}'
        jsonObj = json.loads(jsonObjStr)
        response = self.app.createWidget(jsonObj, "usu-cs5260-sarahjones-bucket3")
        response = json.loads(response)
        self.assertEqual(response['HTTPStatusCode'], 200)
        
     
        
if __name__ == '__main__':
    unittest.main()