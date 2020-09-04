class TestInstancesApi:
    def test_getInstances_success(self, client):
        resp = client.instances.getInstances()

        assert len(resp["instances"]) == 1
        assert isinstance(resp["instances"], list)
        assert isinstance(resp["instances"][0], dict)
        assert (
            resp["instances"][0]["timeSeriesId"][0]
            == "006dfc2d-0324-4937-998c-d16f3b4f1952"
        )
        assert resp["continuationToken"] == "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="
