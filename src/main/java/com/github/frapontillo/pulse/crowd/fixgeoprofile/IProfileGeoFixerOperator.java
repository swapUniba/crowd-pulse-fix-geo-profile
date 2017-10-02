package com.github.frapontillo.pulse.crowd.fixgeoprofile;

import com.github.frapontillo.pulse.crowd.data.entity.Profile;
import com.github.frapontillo.pulse.rx.PulseSubscriber;
import com.github.frapontillo.pulse.spi.IPlugin;
import rx.Observable;
import rx.Subscriber;

/**
 * Rx {@link rx.Observable.Operator} that accepts and outputs {@link Profile}s after attempting a
 * geo-location fix on them.
 * <p/>
 * Clients should implement {@link #getCoordinates(Profile)}.
 *
 * @author Francesco Pontillo
 */
public abstract class IProfileGeoFixerOperator implements Observable.Operator<Profile, Profile> {
    private IPlugin plugin;

    public IProfileGeoFixerOperator(IPlugin plugin) {
        this.plugin = plugin;
    }

    @Override public Subscriber<? super Profile> call(Subscriber<? super Profile> subscriber) {
        return new PulseSubscriber<Profile>(subscriber) {
            @Override public void onNext(Profile profile) {
                plugin.reportElementAsStarted(profile.getId());
                profile = geoFixProfile(profile);
                plugin.reportElementAsEnded(profile.getId());
                subscriber.onNext(profile);
            }

            @Override public void onCompleted() {
                plugin.reportPluginAsCompleted();
                super.onCompleted();
            }

            @Override public void onError(Throwable e) {
                plugin.reportPluginAsErrored();
                super.onError(e);
            }
        };
    }

    /**
     * Fixes the geo-location of a {@link Profile} by calling the actual {@link
     * #getCoordinates(Profile)} implementation.
     *
     * @param profile The {@link Profile} to fix the geo-location for.
     *
     * @return The same input {@link Profile} with an eventual geo-location set to it.
     */
    protected Profile geoFixProfile(Profile profile) {
        Double[] coordinates = getCoordinates(profile);
        if (coordinates != null && coordinates.length == 2) {
            profile.setLatitude(coordinates[0]);
            profile.setLongitude(coordinates[1]);
        }
        return profile;
    }

    /**
     * Actual retrieval of {@link Profile} coordinates happens here.
     *
     * @param profile The {@link Profile} to retrieve coordinates for.
     *
     * @return Array of {@link Double} containing, in order, latitude and longitude.
     */
    public abstract Double[] getCoordinates(Profile profile);

}
