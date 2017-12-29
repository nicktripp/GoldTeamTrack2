from server.Hangman import Hangman
import time
from server.indexing.BTreeIndex import BTreeIndex
from server.indexing.BitmapIndex import BitmapIndex

def print_table(table):
    for row in table:
        rowstr = ""
        for col in row:
            rowstr += "|\t"+col+"\t|"
        print(rowstr)
        rowstr = ""
        for i in range(len(row)):
            rowstr += "\t-\t\t"
        print(rowstr)

def test_print_table():
    table = [["", "50k row", "1m row"],["Q1", "0.3453", "1.0405"], ["Q2", "0.8939", "8.3859"], ["Q3", "2.8495", "40.9859"]]
    print_table(table)

if __name__ == "__main__":

    # print("\n***\n")


    res_table = [["", "1m rows"]]
    res_table.append(["Q1"])
    res_table.append(["Q2"])
    res_table.append(["Q3"])

    # t0 = time.time()
    # query = 'SELECT R.review_id, R.stars, R.useful FROM review50k R WHERE R.stars >= 4 AND R.useful > 20'
    # out = Hangman.execute(query, BTreeIndex)
    # t1 = time.time()
    # elapsed = t1 - t0
    # res_table[1].append("%.5f" % elapsed)
    # print("Time Elapsed %f s over 50k" % (elapsed))
    # q_out = {"Oxz26pqpIb7dDVeuUzNZlg,4,66", "XBAOIjGY9KYBxqU5VzzlpQ,4,27", "rlh_Sk5croW4D1Mqu9CzRg,4,50",
    #          "LCY-SUSHqf2Nt0phPGpKfQ,5,44", "P_RyU43VMJ6S55KwQkEUWQ,4,22", "C5v2RIDy2CsQURHLyZ5RBg,5,26",
    #          "GjoYucYyg3iI14531KSBaw,5,26", "_QoLIsOEgZGrcT51CQ4WRw,5,46", "_ia_lz3xuBUPC_vfis8Lvg,4,53",
    #          "s4JKaA1itibgSSEFUQZJ4g,5,36", "Hlv9lgWbRdHACNkPuzoqAA,4,21", "MiAPRxI-oKg0YTyfpwSdkA,4,30",
    #          "zEIrwM3s6WFjSxJo4pIbdA,4,44", "uZtLQFscS_fQ_TviwuqigQ,5,25", "Gd__caJHVvTHxDU8gu1rTQ,5,26",
    #          "psN6aShEYE8btHF8Dl7eUA,5,33", "rFmgPhkLBFX88cAAnzr5sA,5,69", "Z0sZUsJWPXSH8sEdl8m8MA,5,23",
    #          "gPHLfmR5BHD1QAhA_sWtxw,5,28", "5rhM-NZ2ntapDlme42WmuQ,4,21", "YPjJTgMniGDKlXFpqejD0Q,5,50",
    #          "AqAMF6ew5bHcLRn6RpqCmg,4,44", "i9q4LCyoq_HpWqIW2_SkFA,4,33", "7EHhpntHmKOc_TdXrrOn4g,4,32",
    #          "zq0DxY7uIKObBlHRIu0REw,5,29", "WJoE9AwDVT_92v-hnlNNcA,5,35", "dnio8sXmO-kzeeKmhor1pQ,5,24",
    #          "EhGgl3Iru8HwA1TK9ukcYA,5,99", "i9iVXV-JWqG1ZuSCSPBJJg,5,23", "mEr7Ypam5ypS0jdGrXMIDw,5,28",
    #          "E3x7aKynGK_GFLSuFzu8mg,5,29", "IWwieboI-EKpSOeIqqiISQ,5,31", "I1MUBA6r16Np28xnyRu1QA,5,34",
    #          "zK8w3qhfsREJMCm6edi9ig,5,25", "_1v9DTZcx76qdVQLNqxtZw,4,25", "h9S_aPV5ukVbWeE7MxxvKQ,4,51",
    #          "RpG33CawitvaEb6hZPPuCw,5,21", "GLfPrl_aP5CmzWb4fzXOjA,5,26", "HK0OIdCfKRHsS6jjR2A-tg,5,21",
    #          "0_advxXpmvIhWlGk9n9c0A,4,27", "1iXcFAqQTF0-mnW31AfMUw,5,21", "VG5SHbBoAHXGvxuUAQQAFw,4,24",
    #          "FaxjsxlpGmknbv0OXEowJg,4,71", "JGvnRAMV-Z_CZlZmUa34_A,5,65", "Gyn3_VUwVGrGXn8wQIwnWQ,5,23",
    #          "Z2zkoVqA1R_ATAbyxVfd8Q,5,36", "cm77girmk9CJwd4oOAfb6g,5,24", "u4N_fS_q3wZY-D0ZjJ8qPg,4,24",
    #          "wmWp4bFiWBhtMxDclQv80g,5,26", "0K7ZTxexDxWVNSVMTFjtVQ,5,30", "agWa7j2y9TgJLbdnPOt1iQ,5,32",
    #          "LEWtbvn8nb6RmQE1PhhwVA,5,21", "orne_oYUNL1OEHuCY55N7w,4,23", "7V6H1O1-eHzXkNP2XBN_sA,5,24",
    #          "2cXXSVRNF6NfBLxQde5Vag,4,28",
    #          "l7uqcXQ_dvWfl70JbIQ-mQ,4,25",
    #          "LzjQ7DoKsi_TXcDkFNA9CA,5,30", "LEH3ME7yPXvQl6D7oKaCRA,5,26", "PBhcQU1N5GJZDr_qIKsoOQ,5,53",
    #          "Z-_KjY9k8mOB97tNFgwEMg,4,45", "aSeiTctd0lehY39euzklpQ,5,21", "JgKqxEGbXFw0WFl9BJWTJw,4,70",
    #          "XXVT102bXQkDn3Thi7H6BQ,5,23", "DXfYm30KKSCwk74eKiv3bA,4,21", "r5n4aYq-lE5uXSvEdgvfxg,5,21",
    #          "CWmiHf_EdAgLUd7jMjvNrQ,5,30", "A4IPBr9pb5FHTT0too2jRQ,5,52", "FHUn9LP0WpMUFcuobkfvqw,4,27",
    #          "pqL2QUVaPTiFSNEp-7i9DA,4,49", "Y9UbTFSHV2Unp09_3Pu0LQ,5,21"}
    # out = set(out)
    # for a in out:
    #     if a not in q_out:
    #         print(a)
    # for a in q_out:
    #     if a not in out:
    #         print(a)
    #
    # assert len(q_out) == len(out)
    #
    # print("\n***\n")

    t0 = time.time()
    query = 'SELECT R.review_id, R.stars, R.useful FROM review1m R WHERE R.stars >= 4 AND R.useful > 20'
    out = Hangman.execute(query, BTreeIndex)
    t1 = time.time()
    print("Time Elapsed %f s over 1m" % (t1 - t0))
    elapsed = t1 - t0
    res_table[1].append("%7.4f" % elapsed)

    print("\n***\n")

    # t0 = time.time()
    # query = 'SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business B JOIN review50k R ON (' \
    #         'B.business_id = R.business_id) WHERE B.city = "Champaign" AND B.state = "IL" '
    # out = Hangman.execute(query, BTreeIndex)
    # t1 = time.time()
    # print("Time Elapsed %f s over 50k" % (t1 - t0))
    # q_out = {
    #     "The I.D.E.A. Store,61820,-lYhnHEbHRVBxRZ5TFQdSg,5,0",
    #     "The I.D.E.A. Store,61820,-uwDnbkezvrB1hPOvC_bpA,2,0",
    #     "The I.D.E.A. Store,61820,16utaPhG3y8VFfZF-Kp2BQ,5,2",
    #     "The I.D.E.A. Store,61820,246uVANUXg3N7ZUC_WYMMA,5,1",
    #     "The I.D.E.A. Store,61820,3LAiHB4EbiCF2nK6uVDRug,5,2",
    #     "The I.D.E.A. Store,61820,5rZRMiAb_OOwJMW1VUpUlQ,5,1",
    #     "The I.D.E.A. Store,61820,CiYnp5rL8IlJozV2RT7l5w,5,2",
    #     "The I.D.E.A. Store,61820,FkLY1LVE_Oadui07p3RigQ,5,0",
    #     "The I.D.E.A. Store,61820,G0QAXTlu_cRY4g-OVc16_g,5,3",
    #     "The I.D.E.A. Store,61820,JOR0la2fuFT8EwNO6sRWuw,5,1",
    #     "The I.D.E.A. Store,61820,KgkAuSY37zkRQT_s2Lsi6w,5,1",
    #     "The I.D.E.A. Store,61820,KhfgORM2bjLLEItVdbpU5Q,5,1",
    #     "The I.D.E.A. Store,61820,XniCKbtiOQW6yRsmvhxt1g,5,2",
    #     "The I.D.E.A. Store,61820,tvzLJsRawvM_kGSfkSwgPA,4,0",
    #     "The I.D.E.A. Store,61820,uXCTBJkaULum3oTrNHGWNQ,5,3",
    #     "Lucky Puppy,61820,6V2eu5QsdXcnPDSYMOOwzQ,1,7",
    #     "Lucky Puppy,61820,B7hV5cuAiiTk25JqsMaBRw,1,8",
    #     "Lucky Puppy,61820,SyF-emJLV63aZJ6bvYYVkA,1,10",
    #     "C.O. Daniel's,61820,5LXBQzBafghOFOQ-PB9L-g,2,0",
    #     "C.O. Daniel's,61820,5w-3zT1eHs2tqm-hO0bu4Q,2,6",
    #     "C.O. Daniel's,61820,5zBSh-AvJ3igbVeT9w4d3A,1,2",
    #     "C.O. Daniel's,61820,Ixn8_TZWm6wWH7duZnDYqQ,3,0",
    #     "C.O. Daniel's,61820,JJGlE_9Wvt9PQVn0CVmBaQ,1,0",
    #     "C.O. Daniel's,61820,msD6hRbuUstHxBr_rFzQhw,3,0",
    #     "C.O. Daniel's,61820,rmsrq1s1qlJXbKSift-SMw,4,1",
    #     "C.O. Daniel's,61820,v6LNfMHJlkBs8nF8A94rVA,4,0",
    #     "Espresso Royale,61822,-T7fskbH65dWQkrY5VpS2Q,5,0",
    #     "Espresso Royale,61822,-nqe3osxZVPfGAu-sLmb0w,5,0",
    #     "Espresso Royale,61822,1KAaUNTOwNuGHNCrwn87YQ,4,0",
    #     "Espresso Royale,61822,7rKAnKaUu2FhotIYOR3EGw,3,2",
    #     "Espresso Royale,61822,9WqC2SrEX7zdL9UgtMMWWA,5,1",
    #     "Espresso Royale,61822,MXZhEWczHy3SaOPp0NiKkQ,4,1",
    #     "Espresso Royale,61822,W1lJXoZKcMj3SRTS3beZqg,5,0",
    #     "Espresso Royale,61822,YyH98nPjALt0Z3DT3C9RHw,5,0",
    #     "Espresso Royale,61822,hq7X986ji20Yq_AifG8Jug,5,0",
    #     "Espresso Royale,61822,js8_Ybx6NQt2aok464MhZw,5,0",
    #     "Espresso Royale,61822,l_7q3JrDdOhs9bz1jupF2Q,2,2",
    #     "Espresso Royale,61822,pXA_sQJ8QTwIj8_RgzlcCQ,4,0",
    #     "Espresso Royale,61822,rZ5ZSNacEeUS9uQgn4BXqw,5,0",
    #     "Rush Tan,61821,0G2Al1ShhtNY45tb5fdJpg,5,0",
    #     "Rush Tan,61821,LGfypGhcKCp_6pi9KGpaUQ,3,1",
    #     "Rush Tan,61821,wUoEaBhvhQ8mkKDP10c7yg,5,0",
    #     "Silver Mine Subs,61820,1zdhTbyU0W1wcgaOC8MryA,5,0",
    #     "Silver Mine Subs,61820,3u-_7OCB6AcVTf1qXqkEXA,4,0",
    #     "Silver Mine Subs,61820,5FE_XRKWq-q4BzJuCnzdXw,4,0",
    #     "Silver Mine Subs,61820,8WZSRYm61DMl38a4qtpyfg,5,0",
    #     "Silver Mine Subs,61820,8qS-ktvSkkeRP0iPAkzegw,4,1",
    #     "Silver Mine Subs,61820,ByFpwTwHDzLp43QeEk75Dg,1,0",
    #     "Silver Mine Subs,61820,GI48JexYWGFWyrFEyOCC1A,2,0",
    #     "Silver Mine Subs,61820,JW_SuBmDjk8FJNBvXmeIsQ,3,0",
    #     "Silver Mine Subs,61820,KjQ8QJqGD0Jgk7Y7leprjQ,4,0",
    #     "Silver Mine Subs,61820,Lyblxd6rvjR-1sh3hRafbw,4,1",
    #     "Silver Mine Subs,61820,RZDprwsBrKSht98mZAfyMg,3,0",
    #     "Silver Mine Subs,61820,VF15cvqMYsrSuiDh5Mt7Hw,5,0",
    #     "Silver Mine Subs,61820,VZB49nvct1mEtz0T9D0_VA,4,0",
    #     "Silver Mine Subs,61820,bp_AWBkiTvrIjy27j8A8DA,3,0",
    #     "Silver Mine Subs,61820,meXz_DSTHyktTV_KK7aS0w,5,1",
    #     "Silver Mine Subs,61820,n_UYvLS9_ltKF6Ji8sTsYg,1,0",
    #     "Silver Mine Subs,61820,nv5_dg9iVzsXIsVQgK6FAg,4,1",
    #     "Silver Mine Subs,61820,xpmh1dFr6fCw6gdchygqRw,4,0",
    #     "Baytowne Dental Center,61822,ALqaEGU9EeiuS0wboPhZkA,4,7",
    #     "Baytowne Dental Center,61822,JmR-Ws3HmTZiKUU9SK6Buw,1,0",
    #     "Baytowne Dental Center,61822,W6KLu_GWLa6pZQlpUZsGig,4,0",
    #     "Baytowne Dental Center,61822,btbHUDfQTxw_ozf5GgV0ng,4,1",
    #     "Baytowne Dental Center,61822,eKRUpYp1LSC0h3vIs3kZbQ,1,4",
    #     "Baytowne Dental Center,61822,eUZgogGoC_xRbuKKNXjaCw,1,2",
    #     "ROKs Tacos - Korean BBQ Tacos,,1tdFKbS-RReCyUt_bVKXHg,4,2",
    #     "ROKs Tacos - Korean BBQ Tacos,,HCdvwd91tgjEqhtO1sXZgQ,3,1",
    #     "ROKs Tacos - Korean BBQ Tacos,,Y7InuZCmtP67wlRYZV5MzQ,4,0",
    #     "ROKs Tacos - Korean BBQ Tacos,,t-6zii_y_LuugLYqHHV_gw,3,0",
    #     "ROKs Tacos - Korean BBQ Tacos,,uJ6t922Kz2cgUfPNQ2kB-g,4,0",
    #     "Tradehome Shoes,61820,-nw05LQjvXwYpvMNzEBnGQ,5,0",
    #     "Tradehome Shoes,61820,_3dOzmDOPs-ahy2tSPx8Kg,5,0",
    #     "Tradehome Shoes,61820,rfgsGFGiuiNesrI6-BqddQ,5,0",
    #     "Tradehome Shoes,61820,uE6oI62LoW5kyCbyOJQg1A,5,0",
    #     "TownPlace Suites,61820,-0NfZJmaGp9DJDps9No3tw,5,0",
    #     "TownPlace Suites,61820,7v52bXNVSXNZmRJR4cKD9g,3,0",
    #     "TownPlace Suites,61820,DiLBXncEV4juQt8umUV9-w,3,0",
    #     "TownPlace Suites,61820,Wg7qgsWNVdY6SHkmfn1FDg,2,0",
    #     "TownPlace Suites,61820,XNAiA4b0FS1s309P3MLDFA,4,2",
    #     "TownPlace Suites,61820,YPXKkIjZXy2r8yXYpP1GhQ,5,0",
    #     "TownPlace Suites,61820,Ysq0l2PjnJZw2Dq4vqrKdA,2,0",
    #     "TownPlace Suites,61820,uuD66Q2d4y6TZ-cL07NomQ,5,0",
    #     "TownPlace Suites,61820,xqj6awVbNuGW9AY8N4YHFA,4,2",
    #     "TownPlace Suites,61820,zCDk9wh_puTGTxvHGcqrEA,4,1",
    #     "TownPlace Suites,61820,zo-MSymAaFCLcIB-PwKWOA,2,2",
    #     "My Thai,61821,-pZPn-KuwQ6jUoN9PFDaQQ,5,0",
    #     "My Thai,61821,039RbeJZ_kJwsqZNyOKv3w,3,1",
    #     "My Thai,61821,09uhXcvGha_wXNbyXEMKMA,5,0",
    #     "My Thai,61821,0ESCs774Rd82v2WcDM_ZfA,3,0",
    #     "My Thai,61821,12xRcgtTEhQTWvY3L0hHCg,4,0",
    #     "My Thai,61821,1XgFFtukADqCiylDxAUGWg,5,1",
    #     "My Thai,61821,26rcPHIIDtqKIuYs1aUZ5g,4,0",
    #     "My Thai,61821,4kT5NYTErnZG-AqNSCKFTA,3,0",
    #     "My Thai,61821,6OdximF-dJmLeZBb3b9m9A,2,1",
    #     "My Thai,61821,6oE7Rxbb1gwAedbIiXnbOQ,5,5",
    #     "My Thai,61821,78hxxNbrsZoOHaG7FlDuGw,4,0",
    #     "My Thai,61821,7LjSJn-yBsJsjctm_ZMJ2w,3,1",
    #     "My Thai,61821,7pIGRGFiVOYXcdbxAHkeEQ,3,0",
    #     "My Thai,61821,7rl4EAwmhEzr9S7E7kHYpg,5,1",
    #     "My Thai,61821,87kFfonMYlQNsHkMnw4YDQ,2,1",
    #     "My Thai,61821,8W4-BHaP3fI3HNl0Otf1IA,4,0",
    #     "My Thai,61821,9SDqFUoB5RxIE1k8rnmHRQ,5,5",
    #     "My Thai,61821,BSfa22ch4GrmVATuXJj0Rw,5,1",
    #     "My Thai,61821,CddvzfapjL4Sj1-9TGkOHA,4,1",
    #     "My Thai,61821,CriyGOyIhxt-UImN8DTwmw,3,3",
    #     "My Thai,61821,E-4cxhmfMbLh433XY02sEA,2,0",
    #     "My Thai,61821,ErWFQf5BzoCIdJlxwKTH8g,2,1",
    #     "My Thai,61821,F4YmT11r6l8igqpVcsJaAQ,5,1",
    #     "My Thai,61821,FQtVrQYAJcKK5nafAQQkrw,3,1",
    #     "My Thai,61821,GLUJ8gADp-2D1H8uOorX0g,5,2",
    #     "My Thai,61821,H5TWyZcW4Mob1QwuSdGdjA,4,1",
    #     "My Thai,61821,Hx54o0p70wkrlHnYXwB1NQ,3,2",
    #     "My Thai,61821,IVGgUTq78dI0W0WR2hUXmw,4,0",
    #     "My Thai,61821,InsZSvw1BMtVgBXrctTBnA,5,2",
    #     "My Thai,61821,Ix28xB5zI36eJFpJKLv5hQ,3,0",
    #     "My Thai,61821,IxMo5Gp4xLr7uzFEu4TYxg,1,0",
    #     "My Thai,61821,KoEmsNn3DLo2t3FsOsNSpQ,5,0",
    #     "My Thai,61821,N6kBaiGnn51ohyVdGYhSfA,2,0",
    #     "My Thai,61821,Nb9JJi2cDRCQNrG-sdBEpA,4,0",
    #     "My Thai,61821,O-j5jMMe9KkaLvQ1odHzew,5,0",
    #     "My Thai,61821,RQDl9-M34by-UeDUqhNvnQ,1,0",
    #     "My Thai,61821,SJTkSp5WiAkr-UofcfNVrw,3,0",
    #     "My Thai,61821,ST5BLCTsYZO7JBKqhZZa0Q,5,0",
    #     "My Thai,61821,SfdYitnurpee0atWCSnPng,5,0",
    #     "My Thai,61821,SoYD6rR6nFpy5fKHyYEibg,4,0",
    #     "My Thai,61821,VoT8SnEyk4E-9w-WulmcEA,1,2",
    #     "My Thai,61821,YXvonTMo2bQFAFvmI6Z7uA,4,0",
    #     "My Thai,61821,Z6bG0yTd2qI9cAThFQCvIQ,5,0",
    #     "My Thai,61821,ZieVTkmlHtIy3OudSsmooQ,4,2",
    #     "My Thai,61821,_LGKWIj3WNv3J44iWo3KAQ,5,0",
    #     "My Thai,61821,_NiuTag7p16TcoqTjNP53Q,2,0",
    #     "My Thai,61821,_RViQAwJlVI467x8v3H6QQ,3,0",
    #     "My Thai,61821,aGtJNmkv9Bcl6uMlnv9_5A,3,1",
    #     "My Thai,61821,bx4cwYmo_M08DwVN-2l6LQ,5,0",
    #     "My Thai,61821,c1pooToMOWSaYAXnXw8O-w,2,1",
    #     "My Thai,61821,cBVI287VQVA3MtBMTBbooQ,5,0",
    #     "My Thai,61821,d0gTSs4WrhmVTsP64qlNQA,3,0",
    #     "My Thai,61821,eXH1N3DOO2E1VzyNIuH0nw,5,0",
    #     "My Thai,61821,erEyvVomlt_2zqOVW7s7Yw,4,0",
    #     "My Thai,61821,f3ElvsJ1ueD9NliXUmPM5w,1,0",
    #     "My Thai,61821,fOqDr06eXxDKI3NKP2zEcg,5,1",
    #     "My Thai,61821,fVQRRkOvj3Zg4uTh_GUYwQ,5,1",
    #     "My Thai,61821,fp1XUSuoTTX9BY5bNLBpHQ,2,1",
    #     "My Thai,61821,g7kwMCage7QNtkoy7sD_8Q,3,0",
    #     "My Thai,61821,gNDpkVfPfbh7PZIYD8tdTA,1,1",
    #     "My Thai,61821,go5Lfsc44Kqj955eyKMb7Q,5,0",
    #     "My Thai,61821,hIyQ73vjpjQX-qRHTlTbyw,4,0",
    #     "My Thai,61821,hMnCcH4klsqjwrtD24rbSQ,3,3",
    #     "My Thai,61821,i7fYaKky7An-1j9_KYfg-Q,4,1",
    #     "My Thai,61821,iAgqwsns5bMJ8CvZwshMjg,5,3",
    #     "My Thai,61821,ixQ0FPdhDNnbEmw0lNbUPA,3,0",
    #     "My Thai,61821,kl8Gqt_g5hEpaK4wt3NOBA,5,0",
    #     "My Thai,61821,nhiX-sQ76KdRreIFN9a1Hg,5,1",
    #     "My Thai,61821,nsL9rq4KUtrCql6tls75gw,1,2",
    #     "My Thai,61821,ow-xDISs1e7bpScdC09t6g,3,2",
    #     "My Thai,61821,pscTi2wD4Qg3pAS7wX9JSg,4,1",
    #     "My Thai,61821,qlKbg7B98EeMvEhaUgFcxA,4,0",
    #     "My Thai,61821,rPKNP-Hqo2yJA1xtfvQREg,5,1",
    #     "My Thai,61821,rdHLGefpySAbqp2ZnR_lOA,2,4",
    #     "My Thai,61821,tl6UIqa5RFAh522JokfhuQ,4,0",
    #     "My Thai,61821,tqo_CKkgK13Mctpar8ls_A,3,3",
    #     "My Thai,61821,tzbwPJzBTU4pCJ2xxmomhw,4,1",
    #     "My Thai,61821,wcqw5LcUypowBSYvVkbwHA,1,0",
    #     "My Thai,61821,x8lzHqhPMke0zs3Pc2Skxg,4,2",
    #     "My Thai,61821,xDNSeYAX-HUnSBbomsS4kw,5,1",
    #     "Yori Q Korean Grill & Bar,61820,-K8CQRnJrFqq9Sq1gXQw4A,5,2",
    #     "Yori Q Korean Grill & Bar,61820,-_ku0SZnVpkjbAHFVQNmrg,5,3",
    #     "Yori Q Korean Grill & Bar,61820,-gv4jaaKlvDMB_tyLlN1WA,5,3",
    #     "Yori Q Korean Grill & Bar,61820,14qXmSS6jBUYX3YsxS7u6g,3,0",
    #     "Yori Q Korean Grill & Bar,61820,1u67cvbxT-jYPrcRJ3JAXA,4,0",
    #     "Yori Q Korean Grill & Bar,61820,8AHQHikWGCYZgamFoiwRSw,4,0",
    #     "Yori Q Korean Grill & Bar,61820,9MQPmaWRqqUgVmY-agIr7Q,4,0",
    #     "Yori Q Korean Grill & Bar,61820,Breum7dB7lxc_8jHSfdxuA,4,0",
    #     "Yori Q Korean Grill & Bar,61820,E9jv9xfsjJRPPrMD4FMRcg,2,2",
    #     "Yori Q Korean Grill & Bar,61820,IBHpVAa2sp9lo6p47h_iig,2,0",
    #     "Yori Q Korean Grill & Bar,61820,LO4RkujXppD0UVpe092Jow,4,1",
    #     "Yori Q Korean Grill & Bar,61820,MmQyx-Dygx3a7hTX7_MuyQ,4,1",
    #     "Yori Q Korean Grill & Bar,61820,VzZc4rHGSXYolDQoscOstw,5,0",
    #     "Yori Q Korean Grill & Bar,61820,YPs7GrtrXFx-Nxv6sYDtAw,2,1",
    #     "Yori Q Korean Grill & Bar,61820,YmO3B3QYtQBcLHfcTOd2KQ,2,1",
    #     "Yori Q Korean Grill & Bar,61820,bnj1ZatGJy0L_GXgw0nlrw,5,1",
    #     "Yori Q Korean Grill & Bar,61820,dEH2Zgzo1lYMNzu3xYJ2AA,1,1",
    #     "Yori Q Korean Grill & Bar,61820,e-Bt9aRlP-Z6ypv8SdtP2Q,4,3",
    #     "Yori Q Korean Grill & Bar,61820,hU0d2bPWu2276b5_fhEoSQ,5,10",
    #     "Yori Q Korean Grill & Bar,61820,nWtrPiiVXi92dGYakXr2eA,4,2",
    #     "Yori Q Korean Grill & Bar,61820,oKj9yiBnUwlBOhB_BqqVuQ,4,0",
    #     "Yori Q Korean Grill & Bar,61820,oRfFAAiVouuo_aA0u8-1IQ,4,0",
    #     "Yori Q Korean Grill & Bar,61820,wZtRW5l9Ez8A0-6MBRVCbQ,4,1",
    #     "Yori Q Korean Grill & Bar,61820,wxGjEdwqGnEMBuwBn53J7A,3,0",
    #     "Yori Q Korean Grill & Bar,61820,zHVsbBwliwUFyeYY5PUPDQ,5,3",
    #     "Yori Q Korean Grill & Bar,61820,zLLjruGU8C7j3DLaOPO92g,5,2",
    #     "Za's Italian Cafe,61820,3Q9eJZZTZRNhFwlbh04t4Q,4,0",
    #     "Za's Italian Cafe,61820,50gLYOYjrJxOmTwZNSb1jw,4,1",
    #     "Za's Italian Cafe,61820,DDxndq6dJL04eBT6IhJSjA,4,2",
    #     "Za's Italian Cafe,61820,EtG2G146-GwCN_MRqE5xQQ,4,0",
    #     "Za's Italian Cafe,61820,EtYjnmGap0udMOtS8aPMEQ,2,0",
    #     "Za's Italian Cafe,61820,I9LlBKGjqssTZn0cTlyqRQ,4,2",
    #     "Za's Italian Cafe,61820,NSkPi5kBsmYH9uj__mcvmg,3,0",
    #     "Za's Italian Cafe,61820,SMREjPqSMq-NBOfLm3eApw,4,0",
    #     "Za's Italian Cafe,61820,T-hZw0mcdPQ4YV3dUfNykA,5,1",
    #     "Za's Italian Cafe,61820,TeyYfbILAh2yTSnZrvsqSQ,1,1",
    #     "Za's Italian Cafe,61820,aQoyxeKV6TyM9xgmLpakrA,1,1",
    #     "Za's Italian Cafe,61820,ciLR_ZjR2si8_vH24TxQEw,1,1",
    #     "Za's Italian Cafe,61820,iKAmAMd4A3kIMH2B489CBQ,4,0",
    #     "Za's Italian Cafe,61820,kmi5qbrHQfKTfJuKbMC9dg,3,2",
    #     "Za's Italian Cafe,61820,pzRHNhYny2o7-gImxs4Usg,2,2",
    #     "Za's Italian Cafe,61820,qgOrI9unhXmUxcGnyAokeA,3,0",
    #     "Za's Italian Cafe,61820,xOlzvGqcSOG2apsqiIXcXg,2,4",
    #     "Za's Italian Cafe,61820,zULu0PIIFvfrH3QkTljKWg,1,1"
    # }
    # for a in out:
    #     if a not in q_out:
    #         print(a)
    # for a in q_out:
    #     if a not in out:
    #         print(a)
    #
    # elapsed = t1 - t0
    # res_table[2].append("%.5f" % elapsed)
    #
    # assert len(q_out) == len(out)
    #
    # print("\n***\n")

    t0 = time.time()
    # query = 'SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business B JOIN review1m R ON (B.business_id = R.business_id) WHERE B.city = "Champaign" AND B.state = "IL" '
    query = 'SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business B JOIN review1m R ON ' \
            '(B.business_id = R.business_id) WHERE B.city = "Champaign" AND B.state = "IL" '
    out = Hangman.execute(query, BTreeIndex)
    t1 = time.time()
    print("Time Elapsed %f s over 1m" % (t1 - t0))
    elapsed = t1 - t0
    res_table[2].append("%7.4f" % elapsed)

    print("\n***\n")

    # t0 = time.time()
    # query = 'SELECT DISTINCT B.name FROM business B JOIN review50k R JOIN photos P ON (B.business_id = R.business_id ' \
    #         'AND B.business_id = P.business_id) WHERE B.city = "Champaign" AND B.state = "IL" AND R.stars = 5 AND ' \
    #         'P.label = "inside" '
    # out = Hangman.execute(query, BTreeIndex)
    # t1 = time.time()
    # print("Time Elapsed %f s over 50k" % (t1 - t0))
    # q_out = {'My Thai'}
    # for a in out:
    #     if a not in q_out:
    #         print(a)
    # for a in q_out:
    #     if a not in out:
    #         print(a)
    # elapsed = t1 - t0
    # res_table[3].append("%.5f" % elapsed)
    #
    # assert len(q_out) == len(out)
    #
    # print("\n***\n")

    t0 = time.time()
    # query = 'SELECT DISTINCT B.name FROM business B JOIN review1m R JOIN photos P ON (B.business_id = R.business_id AND B.business_id = P.business_id) WHERE B.city = "Champaign" AND B.state = "IL" AND R.stars = 5 AND P.label = "inside" '
    query = 'SELECT DISTINCT B.name FROM business B JOIN review1m R JOIN photos P ON (B.business_id = R.business_id ' \
            'AND B.business_id = P.business_id) WHERE B.city = "Champaign" AND B.state = "IL" AND R.stars = 5 AND ' \
            'P.label = "inside" '
    out = Hangman.execute(query, BTreeIndex)
    t1 = time.time()
    print("Time Elapsed %f s over 1m" % (t1 - t0))
    elapsed = t1 - t0
    res_table[3].append("%7.4f" % elapsed)

    print("\n***\n***\n")
    print_table(res_table)
