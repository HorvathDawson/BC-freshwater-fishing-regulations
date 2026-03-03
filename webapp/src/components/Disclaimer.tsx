import React, { useEffect } from 'react';
import { X } from 'lucide-react';
import './Disclaimer.css';

interface DisclaimerProps {
    isOpen: boolean;
    onClose: () => void;
}

const Disclaimer: React.FC<DisclaimerProps> = ({ isOpen, onClose }) => {
    // Close on escape key
    useEffect(() => {
        const handleEscape = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && isOpen) onClose();
        };
        window.addEventListener('keydown', handleEscape);
        return () => window.removeEventListener('keydown', handleEscape);
    }, [isOpen, onClose]);

    if (!isOpen) return null;

    return (
        <div className="disclaimer-overlay" onClick={onClose} role="presentation">
            <div className="disclaimer-modal" onClick={e => e.stopPropagation()} role="dialog" aria-modal="true" aria-labelledby="disclaimer-title">
                <button className="disclaimer-close" onClick={onClose} aria-label="Close disclaimer">
                    <X size={20} />
                </button>
                <h2 id="disclaimer-title">Disclaimer</h2>
                <div className="disclaimer-content">
                    <p>
                        This map is provided for <strong>informational purposes only</strong> and should 
                        not be used as a substitute for official regulations or legal advice. This application 
                        is not affiliated with, endorsed by, or in any way officially connected with the 
                        Government of British Columbia, Fisheries and Oceans Canada, or any other government agency.
                    </p>
                    <p>
                        The creator and maintainers of this application do not warrant the reliability, 
                        currency, positional accuracy, or completeness of any data or information published 
                        in this map. The information is provided "as is" without any representations or 
                        warranties of any kind. Regulations may change without notice, and misalignment 
                        of datasets may occur due to the methods used to produce the original products.
                    </p>
                    <p>
                        <strong>Always verify fishing regulations</strong> with the official{' '}
                        <a href="https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations" 
                           target="_blank" rel="noopener noreferrer">
                            BC Fishing Regulations
                        </a>{' '}
                        and any posted notices before fishing. It is your responsibility as an angler 
                        to know and comply with current regulations.
                    </p>
                    <p>
                        Under no circumstances will the creator or maintainers be liable for any direct, 
                        indirect, special, incidental, consequential, or other loss, injury, or damage 
                        caused by use of the information or otherwise arising in connection with this 
                        application. This includes, without limitation, any fines, penalties, lost profits, 
                        or business interruption.
                    </p>
                    <p className="disclaimer-license">
                        Contains information licensed under the{' '}
                        <a href="https://www2.gov.bc.ca/gov/content/data/open-data/open-government-licence-bc" 
                           target="_blank" rel="noopener noreferrer">
                            Open Government Licence – British Columbia
                        </a>.
                    </p>
                </div>
            </div>
        </div>
    );
};

/** Small link button to trigger the disclaimer modal */
export const DisclaimerLink: React.FC<{ onClick: () => void }> = ({ onClick }) => (
    <button className="disclaimer-link" onClick={onClick} aria-label="View disclaimer">
        Disclaimer
    </button>
);

export default Disclaimer;
