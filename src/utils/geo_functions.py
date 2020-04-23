import numpy as np
import pandas as pd

from pdb import set_trace


def wgs_to_eov(latitude, longitude):
    l1 = np.array([-1446726.767770151, 108117.591628471, -3583.808104234472, -2878.331381368081, 5954.115141624551, 600.6820679113122, -162.8630710077665,
                   -1.605983921549304, -167.0351526678922, 94.17357187299571, -1.022705170995968, -0.2102529715747726, 8.374653367902908, -1.669835602681263,
                   -0.5687852934137064, -0.0102364897314749, 3.145747727117474E-03, 4.243461608104641E-02, -0.1220651621472777, 6.164319332818425E-02,
                   -0.0111868486664331])
    l2 = np.array([-4572371.701246022, -52255.9076175919, 116715.2509863826, -1754.425960153494, 4925.95947889304, -802.4934365679912, -120.2800919458708,
                   23.43715857596192, -110.9503311938763, 115.8136579157841, -0.5059586206164071, -0.4012364834317996, 5.931656910019214, -1.601265506199198,
                   -1.274622462961672, -4.413111236599502E-03, 3.290048806865441E-03, 1.958970187546845E-02, -7.879204301801745E-02, 4.433181641666852E-02,
                   -2.052809717688681E-03])

    p = np.array([1,
                  longitude,
                  latitude,
                  longitude * latitude,
                  longitude * longitude,
                  latitude  * latitude,
                  longitude * longitude * longitude,
                  latitude  * latitude  * latitude,
                  longitude * longitude * latitude,
                  longitude * latitude  * latitude,
                  longitude * longitude * longitude * longitude,
                  latitude  * latitude  * latitude  * latitude,
                  longitude * longitude * longitude * latitude,
                  longitude * longitude * latitude  * latitude,
                  longitude * latitude  * latitude  * latitude,
                  longitude * longitude * longitude * longitude * longitude,
                  latitude  * latitude  * latitude  * latitude  * latitude,
                  longitude * longitude * longitude * longitude * latitude,
                  longitude * longitude * longitude * latitude  * latitude,
                  longitude * longitude * latitude  * latitude  * latitude,
                  longitude * latitude  * latitude  * latitude  * latitude]).T

    return p.dot(l1), p.dot(l2)


def eov_to_wgs(x, y):
    """
    Converts EOV coordinates to WGS (GPS) coordinates

    Args:
        x (float): x coordinate in EOV
        y (float): y coordinate in EOV

    Returns:
        longitude (float): lon coordinate in WGS
        latitude (float): lat coordinate in WGS
    """

    l1 = np.array([10.7875129788011,1.261583194250094E-05,-1.337106981037251E-06,2.002452331639742E-12,2.084152383162452E-13,-2.054768579822759E-13,-9.738584419072772E-20,-5.483528404311994E-20,1.23213233745917E-19,3.025094136623967E-19,-1.184716569673144E-26,-1.586266440922264E-26,-5.710893755348441E-26,2.386706396303865E-26,1.022531720857679E-25,4.323474303592353E-33,-2.948540564438182E-32,-7.396192857879473E-33,5.827371979438498E-33,-5.800855076829614E-32,7.038862467233239E-32])
    l2 = np.array([45.03598737541833,9.467557805655573E-07,8.944343409479448E-06,1.513858594286746E-13,-7.156051828482407E-13,5.165384591948798E-15,-2.063583413832688E-20,-4.109505458046855E-20,-1.122684679257782E-19,3.501143099511104E-20,1.028110785949134E-26,-2.622544479899277E-27,-7.552805583186812E-27,-3.616373201962183E-26,1.404861943257385E-26,-1.699287352669952E-33,2.784410147303859E-32,4.57732017597244E-33,3.221377538677363E-33,9.501361872651711E-33,-3.483788985528678E-32])

    p = np.array([1,
                  x,
                  y,
                  x * y,
                  x * x,
                  y * y,
                  x * x * x,
                  y * y * y,
                  x * x * y,
                  x * y * y,
                  x * x * x * x,
                  y * y * y * y,
                  x * x * x * y,
                  x * x * y * y,
                  x * y * y * y,
                  x * x * x * x * x,
                  y * y * y * y * y,
                  x * x * x * x * y,
                  x * x * x * y * y,
                  x * x * y * y * y,
                  x * y * y * y * y]).T

    return p.dot(l1), p.dot(l2)


if __name__ == '__main__':

    assert ((wgs_to_eov(46, 20)[0] - 723792) ** 2 + (wgs_to_eov(46, 20)[1] - 73264) ** 2) ** 0.5 < 100
